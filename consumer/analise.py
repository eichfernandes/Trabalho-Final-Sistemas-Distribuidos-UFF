"""
Calculos do painel (Etapa 5). Funcoes puras sobre um DataFrame de previsoes,
sem Streamlit nem Kafka, para ficarem faceis de testar.
"""
from __future__ import annotations

import time

import numpy as np
import pandas as pd

FAIXAS = ["alto", "medio", "baixo"]
ROTULO = {
    "alto": "Risco alto (< 40%)",
    "medio": "Risco médio (40–60%)",
    "baixo": "Risco baixo (> 60%)",
}
COR = {"alto": "#d62728", "medio": "#ff9f1c", "baixo": "#2ca02c"}

_LIMITES_LEAD = [-1, 7, 30, 90, 180, 365, 10_000]
_ROTULOS_LEAD = ["0–7 dias", "8–30", "31–90", "91–180", "181–365", "> 365"]


def preparar(df: pd.DataFrame) -> pd.DataFrame:
    """Acrescenta colunas derivadas usadas pelo painel."""
    if df.empty:
        return df
    d = df.copy()
    for col in ("noites", "ts_envio_ms", "ts_kafka_ms", "ts_spark_ms", "recv_ms", "desfecho_real"):
        if col not in d:
            d[col] = np.nan
    d["receita_estimada"] = d["adr"] * d["noites"]                  # diaria x noites (euros)
    d["lat_total_ms"] = d["recv_ms"] - d["ts_envio_ms"]             # envio -> painel (mesmo relogio: sua maquina)
    d["lat_spark_ms"] = d["ts_spark_ms"] - d["ts_kafka_ms"]         # chegada ao Kafka -> previsao pronta
    d["faixa_risco"] = pd.Categorical(d["faixa_risco"], categories=FAIXAS, ordered=True)
    d["faixa_antecedencia"] = pd.cut(d["lead_time"], bins=_LIMITES_LEAD, labels=_ROTULOS_LEAD)
    d["prob_pct"] = d["probabilidade"] * 100
    return d


def resumo(d: pd.DataFrame) -> dict:
    contagem = d["faixa_risco"].value_counts().reindex(FAIXAS, fill_value=0)
    soma = d.groupby("faixa_risco", observed=False)["receita_estimada"].sum()
    return {
        "total": len(d),
        "contagem": contagem.to_dict(),
        "receita": soma.to_dict(),
        "receita_total": float(d["receita_estimada"].sum()),
        "sem_noites": bool(d["noites"].isna().any()),
    }


def vazao_recente(d: pd.DataFrame, janela_s: int = 10) -> float:
    agora = time.time() * 1000
    return float((d["recv_ms"] >= agora - janela_s * 1000).sum()) / janela_s


def vazao_por_segundo(d: pd.DataFrame, janela_s: int = 120) -> pd.Series:
    """Previsoes recebidas por segundo nos ultimos `janela_s` segundos (zeros incluidos)."""
    agora = int(time.time())
    seg = (d["recv_ms"] // 1000).astype("int64")
    contagem = seg[seg > agora - janela_s].value_counts()
    serie = contagem.reindex(range(agora - janela_s + 1, agora + 1), fill_value=0).sort_index()
    serie.index = pd.to_datetime(serie.index, unit="s")
    return serie


def risco_por_dimensao(d: pd.DataFrame, coluna: str) -> pd.DataFrame:
    g = d.groupby(coluna, observed=True).agg(
        reservas=("id_reserva", "size"),
        prob_media=("probabilidade", "mean"),
        pct_risco_alto=("faixa_risco", lambda s: (s == "alto").mean() * 100),
    )
    return g.reset_index().sort_values("pct_risco_alto", ascending=False)


def probabilidade_por_antecedencia(d: pd.DataFrame) -> pd.DataFrame:
    g = d.groupby("faixa_antecedencia", observed=True)["prob_pct"].mean().reset_index()
    g.columns = ["antecedencia", "prob_media_pct"]
    return g


def taxa_por_faixa(d: pd.DataFrame) -> pd.DataFrame:
    """Compara a previsao com o que realmente aconteceu (campo desfecho_real)."""
    g = d.groupby("faixa_risco", observed=False).agg(
        reservas=("id_reserva", "size"),
        compareceram=("desfecho_real", "sum"),
    ).reset_index()
    g["taxa_real_pct"] = np.where(g["reservas"] > 0, g["compareceram"] / g["reservas"].replace(0, 1) * 100, np.nan)
    return g


def matriz_confusao(d: pd.DataFrame, limiar: float = 0.5) -> dict:
    v = d.dropna(subset=["desfecho_real"])
    prevista_chega = v["probabilidade"] >= limiar
    real_chegou = v["desfecho_real"] == 1
    vp = int((prevista_chega & real_chegou).sum())
    fp = int((prevista_chega & ~real_chegou).sum())
    fn = int((~prevista_chega & real_chegou).sum())
    vn = int((~prevista_chega & ~real_chegou).sum())
    n = vp + fp + fn + vn
    return {"vp": vp, "fp": fp, "fn": fn, "vn": vn, "n": n, "acuracia": (vp + vn) / n if n else float("nan")}


def latencias(d: pd.DataFrame, limite_ao_vivo_ms: int = 60_000) -> dict:
    """Medianas e p95. A latencia total so vale para reservas recebidas 'ao vivo'
    (quando o painel le um historico antigo, o tempo desde o envio seria enorme)."""
    def est(s: pd.Series):
        s = s.dropna()
        return None if s.empty else {"mediana": float(s.median()), "p95": float(s.quantile(0.95))}

    vivo = d[(d["lat_total_ms"] >= 0) & (d["lat_total_ms"] <= limite_ao_vivo_ms)]
    spark = d[d["lat_spark_ms"] >= 0]["lat_spark_ms"]
    return {"n_vivo": len(vivo), "total": est(vivo["lat_total_ms"]), "spark": est(spark)}


def por_particao(d: pd.DataFrame, coluna: str) -> pd.Series:
    return d.groupby(coluna).size().sort_index()


def alertas(d: pd.DataFrame, faixas: list[str], ordenar_por: str = "probabilidade") -> pd.DataFrame:
    a = d[d["faixa_risco"].isin(faixas)]
    if ordenar_por == "receita":
        return a.sort_values("receita_estimada", ascending=False)
    if ordenar_por == "recentes":
        return a.sort_values("recv_ms", ascending=False)
    return a.sort_values("probabilidade", ascending=True)   # mais critica primeiro
