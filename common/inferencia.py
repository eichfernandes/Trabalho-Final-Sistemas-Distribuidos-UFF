"""
Inferencia do modelo, executada dentro dos executors do Spark (Etapa 4).

Este codigo fica num modulo proprio (e nao dentro do job) de proposito: o Spark
serializa as funcoes do job a cada tarefa, o que descartaria qualquer cache.
Importado de um modulo, o modelo e carregado UMA vez por processo Python do
executor e reaproveitado em todos os micro-lotes seguintes.
"""
import joblib
import pandas as pd

from common.preprocessing import construir_features

_MODELOS = {}


def carregar_modelo(caminho: str):
    if caminho not in _MODELOS:
        modelo = joblib.load(caminho)
        # 1 thread por previsao: o paralelismo vem das tarefas do Spark, nao de dentro do modelo
        modelo.named_steps["classifier"].set_params(n_jobs=1)
        _MODELOS[caminho] = modelo
    return _MODELOS[caminho]


def prever_probabilidades(reservas: pd.DataFrame, caminho_modelo: str, top_countries: list) -> pd.Series:
    """Probabilidade (0 a 1) de o hospede de cada reserva efetivamente chegar."""
    modelo = carregar_modelo(caminho_modelo)
    features = construir_features(reservas, top_countries)
    return pd.Series(modelo.predict_proba(features)[:, 1], index=reservas.index)
