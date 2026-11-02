"""
Etapa 4 - Job de Spark Structured Streaming.

    Kafka (reservas) -> [parse -> limpeza -> Random Forest -> faixa de risco] -> Kafka (previsoes)

  * Le o topico `reservas` em micro-lotes.
  * Descarta mensagens corrompidas e reservas invalidas (mesmas regras do treino).
  * Aplica o Random Forest em paralelo nos executors com uma pandas UDF.
  * Publica o resultado no topico `previsoes`.
  * Usa checkpoint: se o job cair, ao reiniciar ele continua de onde parou, sem perder
    reservas. A escrita no Kafka e "pelo menos uma vez": o lote em andamento na queda pode
    ser reescrito, entao o consumidor deve tolerar previsoes repetidas.

Normalmente e executado pelo Docker Compose (servico `streaming`); veja o README.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

RAIZ = Path(os.environ.get("PROJETO_DIR", Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(RAIZ))

from common.inferencia import prever_probabilidades  # noqa: E402
from common.preprocessing import COLUNAS_EVENTO  # noqa: E402

CAMINHO_MODELO = str(RAIZ / "model" / "modelo_rf.joblib")
CAMINHO_METADADOS = RAIZ / "model" / "metadados.json"

_TEXTO = {"id_reserva", "hotel", "arrival_date_month", "meal", "country", "market_segment",
          "distribution_channel", "reserved_room_type", "deposit_type", "customer_type",
          "data_reserva"}
_DECIMAL = {"adr"}


def _tipo(nome: str):
    if nome in _TEXTO:
        return StringType()
    if nome in _DECIMAL:
        return DoubleType()
    return LongType()


# Esquema do evento publicado pelo producer (mesmos campos do CSV de replay)
ESQUEMA_EVENTO = StructType(
    [StructField(n, _tipo(n), True)
     for n in ["id_reserva"] + COLUNAS_EVENTO + ["data_reserva", "desfecho_real", "ts_envio_ms"]]
)


def parsear_mensagens(raw: DataFrame) -> DataFrame:
    """Converte as linhas cruas do Kafka (value em bytes) em colunas tipadas.

    Mensagens com JSON invalido viram linha com id_reserva nulo e sao descartadas depois.
    """
    return (
        raw.select(
            F.from_json(F.col("value").cast("string"), ESQUEMA_EVENTO).alias("r"),
            (F.col("timestamp").cast("double") * 1000).cast("long").alias("ts_kafka_ms"),
            F.col("partition").alias("particao_origem"),
        )
        .select("r.*", "ts_kafka_ms", "particao_origem")
    )


def transformar(reservas: DataFrame, meta: dict) -> DataFrame:
    """Limpeza + previsao + classificacao de risco. Devolve o DataFrame com colunas key/value."""
    top_countries = meta["top_countries"]
    adr_cap = meta["adr_cap"]
    limiar_alto = meta["limiar_risco_alto"]
    limiar_preservar = meta["limiar_preservar"]

    @pandas_udf(DoubleType())
    def prob_chegar(reserva: pd.DataFrame) -> pd.Series:
        return prever_probabilidades(reserva, CAMINHO_MODELO, top_countries)

    c = F.col
    # Limpeza por linha: mesmas regras de common.preprocessing.registros_validos
    criancas = F.coalesce(c("children"), F.lit(0))
    valida = (
        ((c("adults") + criancas + c("babies")) > 0)
        & ((c("stays_in_weekend_nights") + c("stays_in_week_nights")) > 0)
        & (c("adr") >= 0) & (c("adr") <= adr_cap)
    )
    df = reservas.filter(c("id_reserva").isNotNull() & valida)

    df = df.withColumn("probabilidade", F.round(prob_chegar(F.struct(*COLUNAS_EVENTO)), 4))
    df = df.withColumn(
        "faixa_risco",
        F.when(c("probabilidade") < limiar_alto, "alto")
         .when(c("probabilidade") <= limiar_preservar, "medio")
         .otherwise("baixo"),
    )
    df = df.withColumn("noites", c("stays_in_weekend_nights") + c("stays_in_week_nights"))
    df = df.withColumn("ts_spark_ms", F.expr("unix_millis(current_timestamp())"))

    campos_saida = [
        "id_reserva", "hotel", "data_reserva", "probabilidade", "faixa_risco", "desfecho_real",
        "deposit_type", "market_segment", "customer_type", "lead_time", "noites", "adr",
        "particao_origem", "ts_envio_ms", "ts_kafka_ms", "ts_spark_ms",
    ]
    return df.select(
        c("id_reserva").alias("key"),
        F.to_json(F.struct(*campos_saida)).alias("value"),
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default="kafka:9092", help="endereco do Kafka (visto de dentro do Docker)")
    ap.add_argument("--topico-entrada", default="reservas")
    ap.add_argument("--topico-saida", default="previsoes")
    ap.add_argument("--checkpoint", default="/checkpoints/previsoes")
    ap.add_argument("--inicio", default="earliest", choices=["earliest", "latest"],
                    help="de onde ler na PRIMEIRA execucao (depois, o checkpoint decide)")
    ap.add_argument("--max-por-lote", type=int, default=1000, help="maximo de reservas por micro-lote")
    ap.add_argument("--intervalo", type=int, default=2, help="segundos entre micro-lotes")
    args = ap.parse_args()

    if not Path(CAMINHO_MODELO).exists() or not CAMINHO_METADADOS.exists():
        sys.exit("Modelo nao encontrado em model/. Rode antes o treino: docker compose run --rm treino")
    meta = json.loads(CAMINHO_METADADOS.read_text(encoding="utf-8"))

    spark = (
        SparkSession.builder.appName("previsao-reservas")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topico_entrada)
        .option("startingOffsets", args.inicio)
        .option("maxOffsetsPerTrigger", args.max_por_lote)
        .option("failOnDataLoss", "false")
        .load()
    )
    saida = transformar(parsear_mensagens(raw), meta)

    query = (
        saida.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("topic", args.topico_saida)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=f"{args.intervalo} seconds")
        .start()
    )
    print(f"Streaming ativo: '{args.topico_entrada}' -> '{args.topico_saida}' "
          f"(micro-lotes a cada {args.intervalo}s, ate {args.max_por_lote} reservas por lote).")
    print("Interface do Spark: http://localhost:4040  |  Ctrl+C para parar.\n", flush=True)

    ultimo_lote = -1
    try:
        while query.isActive:
            time.sleep(2)
            p = query.lastProgress
            if p and p["batchId"] != ultimo_lote and p.get("numInputRows", 0) > 0:
                ultimo_lote = p["batchId"]
                print(f"lote {p['batchId']:>4} | {p['numInputRows']:>5} reservas | "
                      f"{p.get('processedRowsPerSecond', 0):7.1f} reservas/s", flush=True)
        if query.exception():
            print("Erro no streaming:", query.exception())
            sys.exit(1)
    except KeyboardInterrupt:
        print("\nParando o streaming...")
        query.stop()


if __name__ == "__main__":
    main()
