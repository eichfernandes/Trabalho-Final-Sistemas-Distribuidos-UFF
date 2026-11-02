"""
Etapa 3 - Producer de reservas.

Le data/reservas_replay.csv (ja em ordem cronologica de reserva) e publica cada
reserva como um evento JSON no topico Kafka `reservas`, simulando o fluxo de
reservas chegando ao hotel.

Uso (a partir da raiz do projeto, com o Kafka no ar):
    python producer/producer.py                      # 5 reservas/segundo, todas as reservas
    python producer/producer.py --limite 100         # so as 100 primeiras (bom para testar)
    python producer/producer.py --taxa 50            # 50 reservas/segundo
    python producer/producer.py --taxa 0             # sem limite de velocidade (teste de carga)

Observacoes:
  * A chave de cada mensagem e o id_reserva. Como os ids sao todos diferentes, o Kafka
    distribui as mensagens de forma uniforme pelas 3 particoes do topico (se a chave
    fosse o tipo de hotel, so 2 valores existiriam e uma particao ficaria vazia).
  * Cada evento recebe o campo `ts_envio_ms` (instante do envio) para medirmos a
    latencia fim a fim na Etapa 6.
  * Rodar o producer duas vezes publica as reservas duas vezes. Para zerar o topico,
    veja a secao "Comandos do dia a dia" do README.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

RAIZ = Path(__file__).resolve().parent.parent
CSV_PADRAO = RAIZ / "data" / "reservas_replay.csv"
# 29092 = porta do Kafka exposta para a SUA maquina (dentro do Docker seria kafka:9092)
BOOTSTRAP_PADRAO = "localhost:29092"
TOPICO_PADRAO = "reservas"


def carregar_eventos(csv: Path, limite: int | None = None) -> list[dict]:
    """Le o CSV de replay e devolve a lista de eventos (dicts), mantendo a ordem."""
    if not csv.exists():
        sys.exit(
            f"Arquivo nao encontrado: {csv}\n"
            "Rode antes o treino (Etapa 2): python model/treinar_modelo.py"
        )
    # Int64 aceita nulos sem transformar 9 em 9.0
    df = pd.read_csv(csv, dtype={"agent": "Int64", "company": "Int64", "children": "Int64"})
    if limite:
        df = df.head(limite)
    # to_json converte NaN / <NA> em null, que e o que o Spark espera
    return json.loads(df.to_json(orient="records", force_ascii=False))


def verificar_kafka(bootstrap: str, topico: str) -> None:
    """Falha cedo, com mensagem clara, se o Kafka estiver fora do ar ou sem o topico."""
    try:
        meta = AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=10)
    except Exception as e:  # noqa: BLE001
        sys.exit(
            f"Nao foi possivel conectar ao Kafka em {bootstrap}: {e}\n"
            "Confira se o Docker esta rodando e se os containers estao no ar: docker compose ps -a"
        )
    if topico not in meta.topics:
        sys.exit(
            f"O topico '{topico}' nao existe. Confira com: docker compose logs kafka-init\n"
            "Se necessario, recrie com: docker compose up -d"
        )
    print(f"Conectado ao Kafka ({bootstrap}). Topico '{topico}': "
          f"{len(meta.topics[topico].partitions)} particoes.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", type=Path, default=CSV_PADRAO, help="CSV de replay")
    ap.add_argument("--bootstrap", default=BOOTSTRAP_PADRAO, help="endereco do Kafka")
    ap.add_argument("--topico", default=TOPICO_PADRAO, help="topico de destino")
    ap.add_argument("--taxa", type=float, default=5.0,
                    help="reservas por segundo (0 = sem limite). Padrao: 5")
    ap.add_argument("--limite", type=int, default=None, help="enviar apenas as N primeiras reservas")
    args = ap.parse_args()

    eventos = carregar_eventos(args.csv, args.limite)
    print(f"{len(eventos):,} reservas carregadas de {args.csv.name}")
    verificar_kafka(args.bootstrap, args.topico)

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "client.id": "producer-reservas",
        "acks": "all",
        "enable.idempotence": True,   # evita duplicatas se houver reenvio por falha de rede
        "linger.ms": 20,              # agrupa mensagens por ate 20 ms (mais eficiente)
    })

    entregues = 0
    erros = 0
    por_particao: Counter = Counter()

    def ao_entregar(err, msg):
        nonlocal entregues, erros
        if err is not None:
            erros += 1
            if erros <= 3:
                print(f"  ERRO de entrega: {err}")
        else:
            entregues += 1
            por_particao[msg.partition()] += 1

    intervalo = 1.0 / args.taxa if args.taxa > 0 else 0.0
    modo = f"{args.taxa:g} reservas/s" if args.taxa > 0 else "velocidade maxima"
    print(f"Enviando para '{args.topico}' a {modo}. Ctrl+C para interromper.\n")

    inicio = time.monotonic()
    ultimo_log = inicio
    enviadas = 0
    try:
        for i, evento in enumerate(eventos):
            if intervalo:
                espera = inicio + i * intervalo - time.monotonic()
                if espera > 0:
                    time.sleep(espera)

            evento["ts_envio_ms"] = int(time.time() * 1000)
            valor = json.dumps(evento, ensure_ascii=False)
            while True:
                try:
                    producer.produce(args.topico, key=evento["id_reserva"], value=valor,
                                     on_delivery=ao_entregar)
                    break
                except BufferError:      # fila interna cheia: espera esvaziar
                    producer.poll(0.5)
            producer.poll(0)
            enviadas += 1

            agora = time.monotonic()
            if agora - ultimo_log >= 1.0:
                taxa_real = enviadas / (agora - inicio)
                print(f"  {enviadas:>7,} enviadas | {taxa_real:7.1f} reservas/s | "
                      f"ultima: {evento['id_reserva']} ({evento['data_reserva']})")
                ultimo_log = agora
    except KeyboardInterrupt:
        print("\nInterrompido pelo usuario.")

    producer.flush(30)   # garante que tudo que esta na fila foi entregue
    duracao = time.monotonic() - inicio
    print("\n=== Resumo ===")
    print(f"Enviadas: {enviadas:,} | Entregues e confirmadas pelo Kafka: {entregues:,} | Erros: {erros}")
    print(f"Duracao: {duracao:.1f}s ({enviadas / duracao:.1f} reservas/s)" if duracao > 0 else "")
    print("Mensagens por particao:", dict(sorted(por_particao.items())))
    if erros or entregues != enviadas:
        sys.exit(1)


if __name__ == "__main__":
    main()
