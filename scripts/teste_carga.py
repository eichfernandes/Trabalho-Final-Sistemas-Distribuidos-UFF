"""
Etapa 6 - Teste de carga: mede vazao e latencia para varias taxas de chegada de reservas.

Para cada taxa, o script envia reservas com o producer e, ao mesmo tempo, le as previsoes que
saem do Spark, calculando:
  * vazao de saida (previsoes/s);
  * latencia de ponta a ponta (do envio pelo producer ate a previsao chegar aqui): p50, p95, p99.
Quando a taxa de chegada passa da capacidade do Spark, as reservas se acumulam no Kafka e a latencia
dispara: esse e o ponto de saturacao. Com a configuracao padrao a capacidade teorica e
(max-por-lote / intervalo) = 1000 / 2 s = 500 reservas/s.

Uso (na raiz do projeto, com o job `streaming` no ar):
    python scripts/teste_carga.py
    python scripts/teste_carga.py --taxas 50 200 400 800 --duracao 20
Resultados tambem salvos em data/resultados_carga.csv.
"""
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

import numpy as np
from confluent_kafka import Consumer, TopicPartition

RAIZ = Path(__file__).resolve().parent.parent
REPLAY = RAIZ / "data" / "reservas_replay.csv"
BOOTSTRAP_PADRAO = "localhost:29092"


def metricas(amostras: list[tuple[int, int, int, int]], esperadas: int) -> dict:
    """amostras = [(recv_ms, ts_envio_ms, ts_kafka_ms, ts_spark_ms)]"""
    if not amostras:
        return {"recebidas": 0, "esperadas": esperadas, "concluiu": False}
    a = np.array(amostras, dtype="int64")
    lat = a[:, 0] - a[:, 1]
    janela_s = max((a[:, 0].max() - a[:, 0].min()) / 1000, 0.001)
    return {
        "recebidas": len(a),
        "esperadas": esperadas,
        "concluiu": len(a) >= esperadas,
        "vazao_saida": len(a) / janela_s,
        "lat_p50_s": float(np.percentile(lat, 50)) / 1000,
        "lat_p95_s": float(np.percentile(lat, 95)) / 1000,
        "lat_p99_s": float(np.percentile(lat, 99)) / 1000,
    }


def rodada(bootstrap: str, taxa: float, duracao: float, espera_max: float) -> dict:
    total_replay = sum(1 for _ in open(REPLAY, encoding="utf-8")) - 1
    esperadas = min(int(taxa * duracao), total_replay)

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"carga-{uuid.uuid4().hex[:8]}",
                  "enable.auto.commit": False})
    try:
        meta = c.list_topics("previsoes", timeout=10)
        # comeca a ler exatamente do fim atual de cada particao (so conta o que esta rodada gerar)
        c.assign([TopicPartition("previsoes", p, c.get_watermark_offsets(TopicPartition("previsoes", p), timeout=10)[1])
                  for p in meta.topics["previsoes"].partitions])
        inicio_ms = int(time.time() * 1000)
        producer = subprocess.Popen(
            [sys.executable, str(RAIZ / "producer" / "producer.py"), "--taxa", str(taxa),
             "--limite", str(esperadas), "--bootstrap", bootstrap],
            cwd=RAIZ, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        amostras = []
        fim_producer = None
        while len(amostras) < esperadas:
            for m in c.consume(num_messages=500, timeout=0.5):
                if m.error():
                    continue
                try:
                    d = json.loads(m.value())
                    if d["ts_envio_ms"] < inicio_ms:   # sobra de rodada anterior
                        continue
                    amostras.append((int(time.time() * 1000), d["ts_envio_ms"], d["ts_kafka_ms"], d["ts_spark_ms"]))
                except (ValueError, KeyError, TypeError):
                    pass
            if producer.poll() is not None:
                fim_producer = fim_producer or time.time()
                if time.time() - fim_producer > espera_max:
                    break
        producer.wait()
        return {"taxa": taxa, **metricas(amostras, esperadas)}
    finally:
        c.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--taxas", type=float, nargs="+", default=[20, 100, 300, 600], help="reservas/s a testar")
    ap.add_argument("--duracao", type=float, default=20, help="segundos de envio por taxa")
    ap.add_argument("--espera-max", type=float, default=90, help="espera maxima pelo fim do processamento (s)")
    ap.add_argument("--bootstrap", default=BOOTSTRAP_PADRAO)
    args = ap.parse_args()
    if not REPLAY.exists():
        sys.exit("data/reservas_replay.csv nao encontrado. Rode antes: docker compose run --rm treino")

    print("Aquecimento (a primeira rodada do Spark e sempre mais lenta; este resultado e descartado)...")
    try:
        rodada(args.bootstrap, 10, 5, 60)
    except Exception as e:  # noqa: BLE001
        sys.exit(f"Falha no teste: {e}\nConfira se o Kafka e o job `streaming` estao no ar.")

    resultados = []
    for taxa in args.taxas:
        print(f"\nRodada: {taxa:g} reservas/s por {args.duracao:g}s ...")
        r = rodada(args.bootstrap, taxa, args.duracao, args.espera_max)
        resultados.append(r)
        print(f"  recebidas {r['recebidas']}/{r['esperadas']}"
              + (f" | vazao {r['vazao_saida']:.0f}/s | latencia p50 {r['lat_p50_s']:.1f}s p95 {r['lat_p95_s']:.1f}s"
                 if r["recebidas"] else ""))

    print("\n=== Resultado do teste de carga ===")
    print(f"{'Taxa (res/s)':>13} {'Recebidas':>10} {'Vazao saida':>12} {'Lat. p50':>9} {'Lat. p95':>9} {'Lat. p99':>9}  Situacao")
    for r in resultados:
        if not r["recebidas"]:
            print(f"{r['taxa']:>13g} {0:>10} {'-':>12} {'-':>9} {'-':>9} {'-':>9}  sem resposta")
            continue
        if not r["concluiu"] or r["lat_p95_s"] >= 10:
            situacao = "SATURADO (latencia crescendo)"
        elif r["vazao_saida"] < 0.9 * r["taxa"]:
            situacao = "NO LIMITE (saida abaixo da chegada)"
        else:
            situacao = "ok"
        print(f"{r['taxa']:>13g} {r['recebidas']:>10} {r['vazao_saida']:>10.0f}/s {r['lat_p50_s']:>8.1f}s "
              f"{r['lat_p95_s']:>8.1f}s {r['lat_p99_s']:>8.1f}s  {situacao}")

    saida = RAIZ / "data" / "resultados_carga.csv"
    with open(saida, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=sorted({k for r in resultados for k in r}))
        w.writeheader()
        w.writerows(resultados)
    print(f"\nResultados salvos em {saida.relative_to(RAIZ)}")


if __name__ == "__main__":
    main()
