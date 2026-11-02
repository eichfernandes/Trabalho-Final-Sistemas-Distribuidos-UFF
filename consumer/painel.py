"""
Etapa 5 - Painel em tempo real (Streamlit).

Le o topico `previsoes` e mostra: visao geral e insights, alertas por faixa de risco,
qualidade do modelo (previsao x realidade) e metricas do sistema distribuido.

Uso (na raiz do projeto, com o ambiente virtual ativo):
    streamlit run consumer/painel.py
e abra http://localhost:8501
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import altair as alt  # noqa: E402
import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from consumer import analise as an  # noqa: E402
from consumer.coletor import ColetorPrevisoes  # noqa: E402

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
TOPICO = os.environ.get("TOPICO_PREVISOES", "previsoes")
DOMINIO_FAIXAS = [an.ROTULO[f] for f in an.FAIXAS]
CORES_FAIXAS = [an.COR[f] for f in an.FAIXAS]

st.set_page_config(page_title="Previsão de reservas em tempo real", page_icon="🏨", layout="wide")


@st.cache_resource
def obter_coletor(bootstrap: str, topico: str) -> ColetorPrevisoes:
    coletor = ColetorPrevisoes(bootstrap, topico)
    coletor.iniciar()
    return coletor


coletor = obter_coletor(BOOTSTRAP, TOPICO)


# ----------------------------- utilidades -----------------------------
def eur(valor: float) -> str:
    return "€ " + f"{valor:,.0f}".replace(",", ".")


def pct(valor: float) -> str:
    return f"{valor:.0f}%"


def segundos(ms: float) -> str:
    return f"{ms / 1000:.1f} s"


def barras_por_faixa(df: pd.DataFrame, valor: str, titulo_y: str, max_y: float | None = None):
    """Grafico de barras com as cores das faixas de risco. `df` tem colunas `faixa` e `valor`."""
    escala_y = alt.Scale(domain=[0, max_y]) if max_y else alt.Undefined
    graf = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("faixa:N", sort=DOMINIO_FAIXAS, title=None, axis=alt.Axis(labelAngle=0)),
            y=alt.Y(f"{valor}:Q", title=titulo_y, scale=escala_y),
            color=alt.Color("faixa:N", legend=None,
                            scale=alt.Scale(domain=DOMINIO_FAIXAS, range=CORES_FAIXAS)),
            tooltip=["faixa", valor],
        )
        .properties(height=280)
    )
    st.altair_chart(graf, width="stretch")


# ----------------------------- seções -----------------------------
def secao_visao_geral(d: pd.DataFrame, r: dict) -> None:
    # Insights gerados automaticamente a partir dos dados
    insights = []
    geral_alto = r["contagem"]["alto"] / r["total"] * 100
    dep = an.risco_por_dimensao(d, "deposit_type")
    dep = dep[dep["reservas"] >= max(5, int(0.01 * len(d)))]
    if not dep.empty:
        top = dep.iloc[0]
        insights.append(
            f"Tipo de depósito mais arriscado: **{top['deposit_type']}**, com {pct(top['pct_risco_alto'])} "
            f"das reservas em risco alto (média geral: {pct(geral_alto)})."
        )
    ant = an.probabilidade_por_antecedencia(d)
    if len(ant) > 1:
        pior = ant.sort_values("prob_media_pct").iloc[0]
        insights.append(
            f"Antecedência com menor chance de comparecimento: **{pior['antecedencia']}** "
            f"(probabilidade média de chegar: {pct(pior['prob_media_pct'])})."
        )
    if r["receita_total"] > 0:
        em_risco = r["receita"]["alto"]
        recuperavel = r["receita"]["medio"]
        insights.append(
            f"**{eur(em_risco)}** ({pct(em_risco / r['receita_total'] * 100)} da receita estimada) está em reservas de "
            f"risco alto; outros **{eur(recuperavel)}** estão em risco médio, onde uma ligação ou oferta pode salvar a reserva."
        )
    if insights:
        st.markdown("**O que os dados mostram agora**\n\n" + "\n".join(f"- {i}" for i in insights))

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Reservas por faixa de risco")
        df_faixa = pd.DataFrame({"faixa": DOMINIO_FAIXAS, "reservas": [r["contagem"][f] for f in an.FAIXAS]})
        barras_por_faixa(df_faixa, "reservas", "Reservas")
    with c2:
        st.subheader("Distribuição da probabilidade de chegar")
        hist = (
            alt.Chart(d[["probabilidade"]])
            .mark_bar(color="#4c78a8")
            .encode(
                x=alt.X("probabilidade:Q", bin=alt.Bin(step=0.05), title="Probabilidade de o hóspede chegar"),
                y=alt.Y("count()", title="Reservas"),
            )
            .properties(height=280)
        )
        st.altair_chart(hist, width="stretch")

    st.subheader("Onde está o risco?")
    opcoes = {"Tipo de depósito": "deposit_type", "Segmento de mercado": "market_segment",
              "Hotel": "hotel", "Tipo de cliente": "customer_type"}
    escolha = st.selectbox("Agrupar por", list(opcoes), key="dimensao")
    g = an.risco_por_dimensao(d, opcoes[escolha])
    graf = (
        alt.Chart(g)
        .mark_bar(color="#d62728")
        .encode(
            y=alt.Y(f"{opcoes[escolha]}:N", sort="-x", title=None),
            x=alt.X("pct_risco_alto:Q", title="% das reservas em risco alto"),
            tooltip=[opcoes[escolha], "reservas", alt.Tooltip("pct_risco_alto:Q", format=".1f"),
                     alt.Tooltip("prob_media:Q", format=".2f")],
        )
        .properties(height=max(120, 36 * len(g)))
    )
    st.altair_chart(graf, width="stretch")

    st.subheader("Antecedência da reserva x probabilidade de comparecer")
    st.bar_chart(ant.set_index("antecedencia")["prob_media_pct"], y_label="Probabilidade média de chegar (%)",
                 x_label="Antecedência (dias entre a reserva e a chegada)")


def secao_alertas(d: pd.DataFrame) -> None:
    st.markdown(
        "**Risco alto (< 40%)**: a reserva provavelmente será perdida, então vale agir primeiro nela. "
        "**Risco médio (40–60%)**: vale ligar ou fazer uma oferta. "
        "**Risco baixo (> 60%)**: preservar a reserva."
    )
    c1, c2, c3 = st.columns([2, 2, 1])
    faixas = c1.multiselect("Faixas", an.FAIXAS, default=["alto", "medio"],
                            format_func=lambda f: an.ROTULO[f], key="alerta_faixas")
    ordem = c2.selectbox("Ordenar por", ["Mais crítica primeiro", "Maior receita em risco", "Mais recentes"],
                         key="alerta_ordem")
    limite = c3.number_input("Linhas", min_value=10, max_value=1000, value=100, step=10, key="alerta_linhas")

    chave = {"Mais crítica primeiro": "probabilidade", "Maior receita em risco": "receita",
             "Mais recentes": "recentes"}[ordem]
    a = an.alertas(d, faixas, chave)
    st.caption(f"Mostrando {min(len(a), int(limite)):,} de {len(a):,} alertas".replace(",", "."))

    tabela = a.head(int(limite)).copy()
    tabela["faixa"] = tabela["faixa_risco"].astype(str).map(an.ROTULO)
    colunas = ["id_reserva", "faixa", "prob_pct", "hotel", "data_reserva", "deposit_type", "market_segment",
               "customer_type", "lead_time", "noites", "adr", "receita_estimada"]
    st.dataframe(
        tabela[colunas],
        hide_index=True,
        width="stretch",
        column_config={
            "id_reserva": "Reserva",
            "faixa": "Faixa",
            "prob_pct": st.column_config.ProgressColumn("Prob. de chegar", min_value=0, max_value=100, format="%.0f%%"),
            "hotel": "Hotel",
            "data_reserva": "Data da reserva",
            "deposit_type": "Depósito",
            "market_segment": "Segmento",
            "customer_type": "Cliente",
            "lead_time": st.column_config.NumberColumn("Antecedência (dias)", format="%d"),
            "noites": st.column_config.NumberColumn("Noites", format="%d"),
            "adr": st.column_config.NumberColumn("Diária (€)", format="%.0f"),
            "receita_estimada": st.column_config.NumberColumn("Receita estimada (€)", format="%.0f"),
        },
    )


def secao_qualidade(d: pd.DataFrame) -> None:
    st.caption(
        "Esta seção compara a previsão com o que realmente aconteceu (campo `desfecho_real`). "
        "Em produção esse resultado só existiria depois da data de chegada; aqui ele existe porque os dados são simulados "
        "a partir de reservas que o modelo nunca viu no treino."
    )
    cm = an.matriz_confusao(d)
    taxa = an.taxa_por_faixa(d)
    t = {f: taxa[taxa["faixa_risco"] == f].iloc[0] for f in an.FAIXAS}

    k1, k2, k3 = st.columns(3)
    k1.metric("Acurácia (limiar de 50%)", pct(cm["acuracia"] * 100) if cm["n"] else "—", help=f"{cm['n']} reservas avaliadas")
    if t["alto"]["reservas"] > 0:
        k2.metric("Risco alto que realmente não compareceu", pct(100 - t["alto"]["taxa_real_pct"]),
                  help="De todas as reservas marcadas como risco alto, quantas de fato não compareceram.")
    if t["baixo"]["reservas"] > 0:
        k3.metric("Risco baixo que realmente compareceu", pct(t["baixo"]["taxa_real_pct"]),
                  help="De todas as reservas marcadas como risco baixo, quantas de fato compareceram.")

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Taxa real de comparecimento por faixa")
        df_t = pd.DataFrame({"faixa": DOMINIO_FAIXAS, "taxa_real_pct": [t[f]["taxa_real_pct"] for f in an.FAIXAS]})
        barras_por_faixa(df_t.dropna(), "taxa_real_pct", "% que realmente compareceu", max_y=100)
        st.caption("Se o modelo funciona, a taxa real cresce de risco alto para risco baixo.")
    with c2:
        st.subheader("Matriz de confusão")
        st.dataframe(
            pd.DataFrame(
                {"Real: cancelou / no-show": [cm["vn"], cm["fn"]], "Real: compareceu": [cm["fp"], cm["vp"]]},
                index=["Previsto: não chega (prob. < 50%)", "Previsto: chega (prob. ≥ 50%)"],
            ),
            width="stretch",
        )
        st.caption("Diagonal principal = acertos. Fora dela: reservas que o modelo errou.")


def secao_sistema(d: pd.DataFrame) -> None:
    lat = an.latencias(d)
    k1, k2, k3 = st.columns(3)
    k1.metric("Vazão (últimos 10 s)", f"{an.vazao_recente(d):.1f} reservas/s")
    if lat["total"]:
        k2.metric("Latência de ponta a ponta", segundos(lat["total"]["mediana"]),
                  help=f"Mediana, do envio pelo producer até a previsão chegar aqui. p95: {segundos(lat['total']['p95'])}. "
                       f"Calculada com {lat['n_vivo']} reservas recebidas ao vivo.")
    else:
        k2.metric("Latência de ponta a ponta", "—", help="Rode o producer com o painel aberto para medir.")
    if lat["spark"]:
        k3.metric("Tempo até a previsão no Spark", segundos(lat["spark"]["mediana"]),
                  help=f"Mediana, da chegada ao Kafka até a previsão pronta. p95: {segundos(lat['spark']['p95'])}.")
    if not lat["total"]:
        st.info("A latência de ponta a ponta só é medida para reservas que chegam enquanto o painel está aberto. "
                "Inicie o producer agora para ver esse número.")

    st.subheader("Previsões por segundo (últimos 2 minutos)")
    st.line_chart(an.vazao_por_segundo(d), y_label="Previsões/s", x_label=None)

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Partições do tópico `reservas`")
        st.bar_chart(an.por_particao(d, "particao_origem"), y_label="Reservas", x_label="Partição")
        st.caption("O Kafka divide as reservas entre as partições; cada partição vira uma tarefa paralela no Spark.")
    with c2:
        st.subheader("Partições do tópico `previsoes`")
        st.bar_chart(an.por_particao(d, "particao_saida"), y_label="Previsões", x_label="Partição")
        st.caption("O mesmo princípio na saída: o consumidor (este painel) lê todas as partições.")


# ----------------------------- página -----------------------------
st.title("🏨 Previsão de cancelamento de reservas — tempo real")

with st.sidebar:
    st.header("Pipeline")
    st.markdown("Producer → Kafka `reservas` → Spark Streaming + Random Forest → Kafka `previsoes` → **este painel**")
    st.caption(f"Kafka: `{BOOTSTRAP}` · tópico: `{TOPICO}`")
    if st.button("Recarregar do início", help="Descarta o que está na tela e lê o tópico `previsoes` desde o começo."):
        coletor.reiniciar()
    st.caption("O painel se atualiza sozinho a cada 2 segundos.")

st.radio("Seção", ["Visão geral", "Alertas", "Qualidade do modelo", "Sistema distribuído"],
         horizontal=True, label_visibility="collapsed", key="secao")


@st.fragment(run_every=2)
def corpo() -> None:
    if coletor.erro:
        st.error(f"Sem conexão com o Kafka em {BOOTSTRAP}: {coletor.erro}. "
                 "Confira se o Docker está rodando (`docker compose ps -a`). Nova tentativa em instantes.")
        return

    d = an.preparar(coletor.dataframe())
    if d.empty:
        st.info("Conectado ao Kafka. Aguardando previsões... Confira se o job `streaming` está rodando e "
                "execute o producer: `python producer/producer.py --taxa 20`")
        return

    r = an.resumo(d)
    k = st.columns(5)
    k[0].metric("Reservas processadas", f"{r['total']:,}".replace(",", "."))
    for col, faixa in zip(k[1:4], an.FAIXAS):
        col.metric(an.ROTULO[faixa], f"{r['contagem'][faixa]:,}".replace(",", "."),
                   delta=f"{r['contagem'][faixa] / r['total'] * 100:.0f}% do total", delta_color="off")
    k[4].metric("Receita estimada em risco alto", eur(r["receita"]["alto"]),
                help="Soma de diária × noites das reservas em risco alto (valores em euros, como no dataset).")
    if coletor.repetidas:
        repetidas = f"{coletor.repetidas:,}".replace(",", ".")
        st.caption(f"{repetidas} previsões repetidas da mesma reserva foram ignoradas "
                   "(o Spark entrega pelo menos uma vez; após uma falha, um lote pode ser reenviado).")
    if r["sem_noites"]:
        st.caption("Algumas previsões antigas não trazem o número de noites; a receita só considera as que trazem.")
    st.divider()

    secao = st.session_state.get("secao", "Visão geral")
    if secao == "Visão geral":
        secao_visao_geral(d, r)
    elif secao == "Alertas":
        secao_alertas(d)
    elif secao == "Qualidade do modelo":
        secao_qualidade(d)
    else:
        secao_sistema(d)


corpo()
