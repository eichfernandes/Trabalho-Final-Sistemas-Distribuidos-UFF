"""
Pre-processamento compartilhado entre o treino (Etapa 2) e o streaming (Etapa 4).

Manter UMA unica implementacao garante que o modelo veja, em producao, exatamente
as mesmas features com que foi treinado.

Tudo aqui e feito LINHA A LINHA (nada depende do dataset inteiro), o que permite
rodar dentro do Spark. As operacoes que dependem do dataset todo (remover
duplicatas, calcular o teto de ADR, escolher os paises mais frequentes) sao feitas
uma vez no treino e guardadas em model/metadados.json.
"""
import numpy as np
import pandas as pd

MESES = {
    "January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
    "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12,
}

NUMERIC_FEATURES = [
    "lead_time", "arrival_month_num", "arrival_date_week_number",
    "arrival_date_day_of_month", "arrival_dayofweek",
    "stays_in_weekend_nights", "stays_in_week_nights", "total_nights",
    "adults", "children", "babies", "total_guests",
    "is_repeated_guest", "previous_cancellations", "previous_bookings_not_canceled",
    "tem_historico_hospede", "taxa_cancelamento_historico",
    "booking_changes", "days_in_waiting_list", "adr",
    "required_car_parking_spaces", "total_of_special_requests",
    "has_agent", "has_company",
]

CATEGORICAL_FEATURES = [
    "hotel", "meal", "country_grouped", "market_segment",
    "distribution_channel", "reserved_room_type", "deposit_type", "customer_type",
]

FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES

# Colunas brutas que compoem o evento de reserva (o que o producer envia ao Kafka).
# Ficam de fora: is_canceled, reservation_status e reservation_status_date (revelam o
# desfecho = vazamento de dados) e assigned_room_type (decidido perto do check-in).
COLUNAS_EVENTO = [
    "hotel", "lead_time", "arrival_date_year", "arrival_date_month",
    "arrival_date_week_number", "arrival_date_day_of_month",
    "stays_in_weekend_nights", "stays_in_week_nights",
    "adults", "children", "babies", "meal", "country", "market_segment",
    "distribution_channel", "is_repeated_guest", "previous_cancellations",
    "previous_bookings_not_canceled", "reserved_room_type", "booking_changes",
    "deposit_type", "agent", "company", "days_in_waiting_list", "customer_type",
    "adr", "required_car_parking_spaces", "total_of_special_requests",
]


def data_chegada(df: pd.DataFrame) -> pd.Series:
    """Data de chegada (check-in) montada a partir de ano / mes (texto) / dia."""
    return pd.to_datetime(
        {
            "year": df["arrival_date_year"].astype(int),
            "month": df["arrival_date_month"].map(MESES).astype(int),
            "day": df["arrival_date_day_of_month"].astype(int),
        }
    )


def data_reserva(df: pd.DataFrame) -> pd.Series:
    """Data em que a reserva foi feita = chegada - lead_time. Define a ordem do replay."""
    return data_chegada(df) - pd.to_timedelta(df["lead_time"].astype(int), unit="D")


def registros_validos(df: pd.DataFrame, adr_cap: float) -> pd.Series:
    """Mascara booleana das reservas validas (secao 7.3 do notebook).

    Invalida: sem hospedes, sem noites, ADR negativo ou acima do teto.
    """
    criancas = df["children"].fillna(0)
    hospedes = df["adults"] + criancas + df["babies"]
    noites = df["stays_in_weekend_nights"] + df["stays_in_week_nights"]
    return (hospedes > 0) & (noites > 0) & (df["adr"] >= 0) & (df["adr"] <= adr_cap)


def construir_features(df: pd.DataFrame, top_countries: list) -> pd.DataFrame:
    """Transforma reservas brutas nas features do modelo (secoes 7.2, 7.4 e 8.1)."""
    out = df.copy()

    # Valores faltantes (7.2)
    out["children"] = out["children"].fillna(0)
    out["country"] = out["country"].fillna("Unknown")
    out["has_agent"] = out["agent"].notnull().astype(int)
    out["has_company"] = out["company"].notnull().astype(int)

    # Variaveis derivadas (7.4)
    out["total_nights"] = out["stays_in_weekend_nights"] + out["stays_in_week_nights"]
    out["total_guests"] = out["adults"] + out["children"] + out["babies"]

    anteriores = out["previous_cancellations"] + out["previous_bookings_not_canceled"]
    out["tem_historico_hospede"] = (anteriores > 0).astype(int)
    out["taxa_cancelamento_historico"] = np.where(
        anteriores > 0, out["previous_cancellations"] / anteriores.replace(0, 1), 0.0
    )

    # Sazonalidade (8.1): mes e dia da semana, que se repetem todo ano
    chegada = data_chegada(out)
    out["arrival_month_num"] = chegada.dt.month
    out["arrival_dayofweek"] = chegada.dt.dayofweek

    # Pais: top N mantidos, resto vira "Other" (8.1)
    out["country_grouped"] = np.where(out["country"].isin(top_countries), out["country"], "Other")

    return out[FEATURES]
