"""
Q1 TIME-Optimized Implementation using Polars

Este módulo contiene la implementación optimizada por velocidad para Q1:
- Encuentra las top 10 fechas con más tweets
- Para cada fecha, identifica el usuario con más tweets

Estrategia:
- Carga completa en memoria con Polars (scan_ndjson + collect)
- Lazy evaluation seguido de materialización temprana
- Operaciones vectorizadas sobre DataFrame completo
- Trade-off: velocidad máxima a costa de ~130 MB de RAM

Complejidad:
- Tiempo: O(n log n) por los sorts
- Espacio: O(n) por DataFrame en memoria
"""

from datetime import datetime, date
from typing import List, Tuple
import polars as pl


def q1_time(file_path: str) -> List[Tuple[date, str]]:
    """
    Retorna las top 10 fechas con más tweets y el usuario más activo por fecha.

    Implementación TIME-optimized usando Polars con carga completa en memoria.

    Args:
        file_path: Ruta al archivo NDJSON con tweets

    Returns:
        Lista de tuplas (fecha, username) ordenadas por:
        1. Cantidad de tweets (descendente)
        2. Fecha (ascendente) como tie-breaker

    Ejemplo:
        >>> result = q1_time("tweets.json")
        >>> result[0]
        (datetime.date(2021, 2, 12), 'RanbirS00614606')
    """
    # Carga completa en memoria: scan lazy + collect eager
    # Extrae solo las columnas necesarias para eficiencia
    df = pl.scan_ndjson(file_path).select([
        pl.col("date").str.slice(0, 10).alias("date_only"),
        pl.col("user").struct.field("username").alias("username")
    ]).filter(
        pl.col("username").is_not_null() &
        pl.col("date_only").is_not_null()
    ).collect()

    # Encuentra las top 10 fechas por cantidad de tweets
    # Sort estable: primero por count desc, luego por date asc (tie-breaker)
    top_dates = (
        df
        .group_by("date_only")
        .agg(pl.len().alias("tweet_count"))
        .sort(["tweet_count", "date_only"], descending=[True, False])
        .head(10)
    )

    results = []

    # Para cada top fecha, encuentra el usuario más activo
    for row in top_dates.iter_rows(named=True):
        date_str = row["date_only"]

        # Filtra tweets de esta fecha específica
        date_df = df.filter(pl.col("date_only") == date_str)

        # Encuentra el usuario con más tweets en esta fecha
        # Sort estable: primero por count desc, luego por username asc (tie-breaker)
        top_user = (
            date_df
            .group_by("username")
            .agg(pl.len().alias("user_tweet_count"))
            .sort(["user_tweet_count", "username"], descending=[True, False])
            .head(1)
        )

        username = top_user["username"][0]
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()

        results.append((date_obj, username))

    return results
