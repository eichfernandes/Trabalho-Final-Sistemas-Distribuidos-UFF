"""
Etapa 5 - Consumidor do topico `previsoes`.

Le as previsoes publicadas pelo Spark em uma thread de fundo e as mantem em memoria
para o painel. A cada inicio, le o topico DESDE O COMECO (sem confirmar offsets):
isso reconstroi todo o historico a partir do Kafka, que guarda as mensagens em disco.
"""
from __future__ import annotations

import json
import threading
import time

import pandas as pd
from confluent_kafka import Consumer

GRUPO = "painel-reservas"


class ColetorPrevisoes:
    def __init__(self, bootstrap: str, topico: str = "previsoes"):
        self.bootstrap = bootstrap
        self.topico = topico
        self.conectado = False
        self.erro: str | None = None
        self._registros: list[dict] = []
        self._lock = threading.Lock()
        self._parar = threading.Event()
        self._thread: threading.Thread | None = None
        self._df_cache: pd.DataFrame = pd.DataFrame()
        self._df_cache_n = -1
        self.repetidas = 0   # previsoes repetidas da mesma reserva (ignoradas)

    # ---------- ciclo de vida ----------
    def iniciar(self) -> None:
        self._parar.clear()
        self._thread = threading.Thread(target=self._loop, name="coletor-previsoes", daemon=True)
        self._thread.start()

    def reiniciar(self) -> None:
        """Descarta o estado e le o topico de novo desde o inicio."""
        self._parar.set()
        if self._thread:
            self._thread.join(timeout=10)
        with self._lock:
            self._registros = []
        self._df_cache, self._df_cache_n = pd.DataFrame(), -1
        self.repetidas = 0
        self.iniciar()

    # ---------- dados ----------
    def adicionar(self, registro: dict) -> None:
        with self._lock:
            self._registros.append(registro)

    @property
    def total(self) -> int:
        return len(self._registros)

    def dataframe(self) -> pd.DataFrame:
        """DataFrame com as previsoes recebidas, uma por reserva (reconstruido so quando chega algo novo).

        O Spark escreve no Kafka com garantia "pelo menos uma vez": apos uma falha, o lote que
        estava em andamento pode ser publicado de novo. Por isso o consumidor e idempotente:
        se a mesma reserva aparece duas vezes, vale a ultima previsao.
        """
        with self._lock:
            n = len(self._registros)
            if n == self._df_cache_n:
                return self._df_cache
            copia = list(self._registros)
        df = pd.DataFrame.from_records(copia) if copia else pd.DataFrame()
        if "id_reserva" in df:
            bruto = len(df)
            df = df.drop_duplicates("id_reserva", keep="last").reset_index(drop=True)
            self.repetidas = bruto - len(df)
        self._df_cache = df
        self._df_cache_n = n
        return self._df_cache

    # ---------- thread de leitura ----------
    def _loop(self) -> None:
        while not self._parar.is_set():
            consumidor = None
            try:
                consumidor = Consumer({
                    "bootstrap.servers": self.bootstrap,
                    "group.id": GRUPO,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                })
                meta = consumidor.list_topics(timeout=5)
                if self.topico not in meta.topics:
                    raise RuntimeError(f"topico '{self.topico}' nao existe")
                consumidor.subscribe([self.topico])
                self.conectado, self.erro = True, None

                while not self._parar.is_set():
                    for msg in consumidor.consume(num_messages=500, timeout=0.5):
                        if msg.error():
                            continue
                        try:
                            registro = json.loads(msg.value())
                        except (ValueError, TypeError):
                            continue   # mensagem invalida: ignora
                        registro["recv_ms"] = int(time.time() * 1000)
                        registro["particao_saida"] = msg.partition()
                        self.adicionar(registro)
            except Exception as e:  # noqa: BLE001
                self.conectado = False
                self.erro = f"{type(e).__name__}: {e}"
                self._parar.wait(5)   # tenta reconectar em 5 s
            finally:
                if consumidor is not None:
                    try:
                        consumidor.close()
                    except Exception:  # noqa: BLE001
                        pass
