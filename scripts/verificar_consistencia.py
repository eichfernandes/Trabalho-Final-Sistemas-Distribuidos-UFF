"""
Etapa 6 - Verificacao de consistencia entre os topicos `reservas` e `previsoes`.

Confere se cada reserva publicada gerou uma previsao:
  * reservas SEM previsao  = perda de dados  (nao pode acontecer);
  * previsoes REPETIDAS    = duplicatas (possiveis apos uma falha: o Spark escreve no Kafka
                             com garantia "pelo menos uma vez", entao o lote que estava em
                             andamento quando o job caiu pode ser escrito de novo).

Uso (na raiz do projeto, com o ambiente virtual ativo):
    python scripts/verificar_consistencia.py
Codigo de saida: 0 = nenhuma reserva perdida | 1 = ha reservas sem previsao.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from collections import Counter

from confluent_kafka import Consumer, TopicPartition

BOOTSTRAP_PADRAO = "localhost:29092"


def _consumidor(bootstrap: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"verificador-{uuid.uuid4().hex[:8]}",
        "enable.auto.commit": False,
    })


def limites(consumidor: Consumer, topico: str) -> dict[int, tuple[int, int]]:
    """{particao: (offset_inicial, offset_final)} do topico."""
    meta = consumidor.list_topics(topico, timeout=10)
    if topico not in meta.topics or meta.topics[topico].error is not None:
        raise RuntimeError(f"topico '{topico}' nao encontrado")
    return {
        p: consumidor.get_watermark_offsets(TopicPartition(topico, p), timeout=10)
        for p in meta.topics[topico].partitions
    }


def contar_mensagens(bootstrap: str, topico: str) -> int:
    c = _consumidor(bootstrap)
    try:
        return sum(hi - lo for lo, hi in limites(c, topico).values())
    finally:
        c.close()


def ler_ids(bootstrap: str, topico: str, ocioso_max_s: float = 15.0) -> tuple[Counter, dict[int, int]]:
    """Le o topico INTEIRO e devolve (contagem de cada id_reserva, mensagens por particao)."""
    c = _consumidor(bootstrap)
    try:
        lim = limites(c, topico)
        por_particao = {p: hi - lo for p, (lo, hi) in lim.items()}
        c.assign([TopicPartition(topico, p, lo) for p, (lo, _) in lim.items()])
        pendentes = {p for p, (lo, hi) in lim.items() if hi > lo}
        ids: Counter = Counter()
        ultimo_progresso = time.time()
        while pendentes and time.time() - ultimo_progresso < ocioso_max_s:
            msgs = c.consume(num_messages=1000, timeout=1.0)
            for m in msgs:
                if m.error():
                    continue
                ultimo_progresso = time.time()
                try:
                    ids[json.loads(m.value())["id_reserva"]] += 1
                except (ValueError, KeyError, TypeError):
                    pass
                if m.offset() + 1 >= lim[m.partition()][1]:
                    pendentes.discard(m.partition())
        return ids, por_particao
    finally:
        c.close()


def verificar(bootstrap: str = BOOTSTRAP_PADRAO, silencioso: bool = False) -> dict:
    entrada, part_in = ler_ids(bootstrap, "reservas")
    saida, part_out = ler_ids(bootstrap, "previsoes")

    perdidas = sum(max(0, n - saida.get(i, 0)) for i, n in entrada.items())
    repetidas = sum(max(0, n - entrada.get(i, 0)) for i, n in saida.items())
    r = {
        "reservas": sum(entrada.values()),
        "previsoes": sum(saida.values()),
        "previsoes_distintas": len(saida),
        "perdidas": perdidas,
        "repetidas": repetidas,
        "particoes_reservas": dict(sorted(part_in.items())),
        "particoes_previsoes": dict(sorted(part_out.items())),
        "ok": perdidas == 0,
    }
    if not silencioso:
        imprimir(r)
    return r


def imprimir(r: dict) -> None:
    print("\n=== Consistencia entre 'reservas' e 'previsoes' ===")
    print(f"Mensagens em 'reservas':   {r['reservas']:>7,}   por particao: {r['particoes_reservas']}")
    print(f"Mensagens em 'previsoes':  {r['previsoes']:>7,}   por particao: {r['particoes_previsoes']}")
    print(f"Previsoes de reservas distintas: {r['previsoes_distintas']:,}")
    print(f"Reservas SEM previsao (perdidas): {r['perdidas']}")
    print(f"Previsoes REPETIDAS (duplicatas): {r['repetidas']}")
    if r["ok"] and r["repetidas"] == 0:
        print("\nRESULTADO: OK - toda reserva gerou exatamente uma previsao.")
    elif r["ok"]:
        print("\nRESULTADO: OK - nenhuma reserva perdida. As duplicatas sao esperadas apos uma falha "
              "(entrega 'pelo menos uma vez'); o painel as ignora pelo id_reserva.")
    else:
        print("\nRESULTADO: FALHA - ha reservas sem previsao. O job ainda pode estar processando "
              "(espere alguns segundos e rode de novo) ou o tópico foi recriado sem zerar o checkpoint.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default=BOOTSTRAP_PADRAO)
    args = ap.parse_args()
    try:
        r = verificar(args.bootstrap)
    except Exception as e:  # noqa: BLE001
        sys.exit(f"Nao foi possivel verificar: {e}\nConfira se o Docker/Kafka estao no ar (docker compose ps -a).")
    sys.exit(0 if r["ok"] else 1)


if __name__ == "__main__":
    main()
