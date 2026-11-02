# Imagem unica usada por: spark-master, spark-worker-1/2, streaming (job) e treino.
# Usar a MESMA imagem em todos garante as mesmas versoes de Python, pandas e
# scikit-learn no treino do modelo e nos executors que fazem a inferencia
# (o arquivo .joblib do modelo so e confiavel se as versoes forem iguais).
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/* \
 # caminho fixo para o JAVA_HOME, independente da arquitetura (amd64 / arm64)
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

COPY spark/requirements-spark.txt /tmp/requirements-spark.txt
RUN pip install --no-cache-dir -r /tmp/requirements-spark.txt

ENV JAVA_HOME=/opt/java \
    SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark \
    PYSPARK_PYTHON=python3 \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1
ENV PATH="${SPARK_HOME}/bin:${PATH}"

WORKDIR /app
