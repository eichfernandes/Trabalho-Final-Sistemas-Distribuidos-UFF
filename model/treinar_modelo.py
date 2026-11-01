"""
Etapa 2 - Preparo dos dados e treino do Random Forest (executado uma unica vez, offline).

O que este script faz:
  1. Carrega o dataset Hotel Booking Demand (baixa do Kaggle se ainda nao existir).
  2. Limpa os dados (duplicatas, reservas invalidas, teto de ADR).
  3. Separa 80% para treino e 20% para teste (estratificado).
  4. Treina o Random Forest e avalia no conjunto de teste.
  5. Salva o modelo e os metadados necessarios para o streaming.
  6. Gera o dataset de replay a partir do conjunto de TESTE, em ordem cronologica.

Por que o replay usa so o conjunto de teste: o modelo nunca viu essas reservas,
entao a acuracia que o painel mostrar em tempo real e honesta.

Uso (a partir da raiz do projeto):
    python model/treinar_modelo.py
    python model/treinar_modelo.py --csv caminho/para/hotel_bookings.csv
"""
from __future__ import annotations

import argparse
import json
import platform
import shutil
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, confusion_matrix, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from common.preprocessing import (  # noqa: E402
    CATEGORICAL_FEATURES, COLUNAS_EVENTO, FEATURES, NUMERIC_FEATURES,
    construir_features, data_reserva, registros_validos,
)

DATA_DIR = RAIZ / "data"
MODEL_DIR = RAIZ / "model"
CSV_PADRAO = DATA_DIR / "hotel_bookings.csv"
SEED = 42
N_PAISES = 15

# Faixas de risco definidas no projeto (probabilidade de o hospede CHEGAR)
LIMIAR_RISCO_ALTO = 0.40
LIMIAR_PRESERVAR = 0.60

# Hiperparametros: versao enxuta do notebook (300 arvores, folhas de 2 amostras).
# Com aquela configuracao o arquivo do modelo passa de varios centenas de MB, o que
# pesaria muito para carregar em cada executor do Spark. Esta versao fica na casa de
# poucas dezenas de MB, com desempenho praticamente igual.
RF_PARAMS = dict(
    n_estimators=100,
    max_depth=20,
    min_samples_leaf=10,
    class_weight="balanced",
    random_state=SEED,
    n_jobs=-1,
)


def carregar_dataset(csv: Path | None) -> pd.DataFrame:
    if csv is None:
        csv = CSV_PADRAO
        if not csv.exists():
            print("Dataset nao encontrado em data/. Baixando do Kaggle (kagglehub)...")
            try:
                import kagglehub
                pasta = kagglehub.dataset_download("jessemostipak/hotel-booking-demand")
            except Exception as e:  # noqa: BLE001
                sys.exit(
                    f"Falha ao baixar o dataset: {e}\n"
                    "Baixe manualmente em https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand\n"
                    f"e coloque o arquivo hotel_bookings.csv em: {CSV_PADRAO}"
                )
            DATA_DIR.mkdir(exist_ok=True)
            shutil.copy(Path(pasta) / "hotel_bookings.csv", CSV_PADRAO)
    df = pd.read_csv(csv)
    print(f"Dataset carregado: {df.shape[0]:,} linhas x {df.shape[1]} colunas")
    return df


def limpar(df: pd.DataFrame):
    """Limpeza dependente do dataset inteiro. Devolve (df_limpo, adr_cap)."""
    n0 = len(df)
    df = df.drop_duplicates().copy()
    print(f"Duplicatas removidas: {n0 - len(df):,}")

    # Teto de ADR = percentil 99,9 (calculado depois de tirar reservas sem hospedes e ADR negativo)
    hospedes = df["adults"] + df["children"].fillna(0) + df["babies"]
    base = df[(hospedes > 0) & (df["adr"] >= 0)]
    adr_cap = float(base["adr"].quantile(0.999))

    validos = registros_validos(df, adr_cap)
    print(f"Reservas invalidas removidas: {(~validos).sum():,} (teto de ADR = {adr_cap:.2f})")
    df = df[validos].copy()
    print(f"Reservas apos limpeza: {len(df):,}")
    return df, adr_cap


def montar_modelo() -> Pipeline:
    preprocessor = ColumnTransformer([
        ("num", "passthrough", NUMERIC_FEATURES),
        ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
    ])
    return Pipeline([
        ("preprocessor", preprocessor),
        ("classifier", RandomForestClassifier(**RF_PARAMS)),
    ])


def faixa(p: np.ndarray) -> np.ndarray:
    return np.where(p < LIMIAR_RISCO_ALTO, "alto", np.where(p <= LIMIAR_PRESERVAR, "medio", "baixo"))


def gerar_replay(df_teste: pd.DataFrame) -> Path:
    """Gera o CSV que o producer vai reproduzir, ordenado pela data em que a reserva foi feita."""
    replay = df_teste[COLUNAS_EVENTO].copy()
    replay["data_reserva"] = data_reserva(df_teste).dt.strftime("%Y-%m-%d")
    # 1 = hospede compareceu (check-out), 0 = cancelou/no-show. Usado SO para comparar no painel.
    replay["desfecho_real"] = df_teste["lead_convertido"].astype(int)
    replay = replay.sort_values("data_reserva", kind="stable").reset_index(drop=True)
    replay.insert(0, "id_reserva", [f"R{i:06d}" for i in range(1, len(replay) + 1)])
    for col in ("agent", "company", "children"):  # evita "9.0" no lugar de "9" (e mantem nulos)
        replay[col] = replay[col].astype("Int64")
    caminho = DATA_DIR / "reservas_replay.csv"
    DATA_DIR.mkdir(exist_ok=True)
    replay.to_csv(caminho, index=False)
    print(f"Replay salvo: {caminho.relative_to(RAIZ)} ({len(replay):,} reservas, "
          f"de {replay['data_reserva'].iloc[0]} a {replay['data_reserva'].iloc[-1]})")
    return caminho


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", type=Path, default=None, help="caminho do hotel_bookings.csv (opcional)")
    args = ap.parse_args()

    print("\n=== 1. Carga e limpeza ===")
    df = carregar_dataset(args.csv)
    df, adr_cap = limpar(df)

    df["lead_convertido"] = 1 - df["is_canceled"]   # 1 = chegou, 0 = cancelou/no-show
    print(f"Taxa de conversao (hospede chegou): {df['lead_convertido'].mean():.1%}")

    top_countries = (
        df["country"].fillna("Unknown").value_counts().nlargest(N_PAISES).index.tolist()
    )

    print("\n=== 2. Divisao treino/teste (80/20, estratificada) ===")
    df_treino, df_teste = train_test_split(
        df, test_size=0.2, random_state=SEED, stratify=df["lead_convertido"]
    )
    print(f"Treino: {len(df_treino):,} | Teste: {len(df_teste):,}")

    print("\n=== 3. Treino do Random Forest ===")
    X_treino = construir_features(df_treino, top_countries)
    X_teste = construir_features(df_teste, top_countries)
    y_treino, y_teste = df_treino["lead_convertido"], df_teste["lead_convertido"]

    modelo = montar_modelo()
    modelo.fit(X_treino, y_treino)

    print("\n=== 4. Avaliacao no conjunto de teste ===")
    proba = modelo.predict_proba(X_teste)[:, 1]   # probabilidade de o hospede CHEGAR
    pred = (proba >= 0.5).astype(int)
    auc = roc_auc_score(y_teste, proba)
    acc = accuracy_score(y_teste, pred)
    tn, fp, fn, tp = confusion_matrix(y_teste, pred).ravel()
    print(f"ROC-AUC:  {auc:.4f}")
    print(f"Acuracia: {acc:.4f} (limiar 0,5)")
    print(f"Matriz de confusao -> VN={tn} FP={fp} FN={fn} VP={tp}")

    faixas = pd.Series(faixa(proba)).value_counts(normalize=True)
    print("Distribuicao nas faixas de risco (teste):")
    print(f"   alto  (< 40%):   {faixas.get('alto', 0):.1%}")
    print(f"   medio (40-60%):  {faixas.get('medio', 0):.1%}")
    print(f"   baixo (> 60%):   {faixas.get('baixo', 0):.1%}")
    print(f"Probabilidade media: chegaram = {proba[y_teste.values == 1].mean():.1%} | "
          f"nao chegaram = {proba[y_teste.values == 0].mean():.1%}")

    print("\n=== 5. Salvando artefatos ===")
    MODEL_DIR.mkdir(exist_ok=True)
    caminho_modelo = MODEL_DIR / "modelo_rf.joblib"
    joblib.dump(modelo, caminho_modelo, compress=3)
    tamanho_mb = caminho_modelo.stat().st_size / 1e6
    print(f"Modelo salvo: {caminho_modelo.relative_to(RAIZ)} ({tamanho_mb:.1f} MB)")

    metadados = {
        "top_countries": top_countries,
        "adr_cap": adr_cap,
        "features": FEATURES,
        "limiar_risco_alto": LIMIAR_RISCO_ALTO,
        "limiar_preservar": LIMIAR_PRESERVAR,
        "metricas_teste": {"roc_auc": round(auc, 4), "acuracia": round(acc, 4)},
        "versoes": {
            "python": platform.python_version(),
            "scikit-learn": sklearn.__version__,
            "pandas": pd.__version__,
            "numpy": np.__version__,
        },
    }
    caminho_meta = MODEL_DIR / "metadados.json"
    caminho_meta.write_text(json.dumps(metadados, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Metadados salvos: {caminho_meta.relative_to(RAIZ)}")

    gerar_replay(df_teste)
    print("\nConcluido.")


if __name__ == "__main__":
    main()
