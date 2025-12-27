"""
Q1 MEMORY-Optimized Implementation using Polars

Este módulo contiene la implementación optimizada por memoria para Q1:
- Encuentra las top 10 fechas con más tweets
- Para cada fecha, identifica el usuario con más tweets

Estrategia:
- Mantiene lazy evaluation sin materializar el DataFrame completo
- Ejecuta múltiples scans del archivo (1 para fechas + 10 para usuarios)
- Solo materializa resultados pequeños (top 10 fechas, 1 usuario por fecha)
- Trade-off: mayor tiempo de ejecución (~3.4s) a cambio de ~7 MB de RAM

Complejidad:
- Tiempo: O(11n) por los 11 scans del archivo
- Espacio: O(1) - solo almacena resultados agregados pequeños
"""

from datetime import datetime, date
from typing import List, Tuple
import polars as pl


def q1_memory(file_path: str) -> List[Tuple[date, str]]:
    """
    Retorna las top 10 fechas con más tweets y el usuario más activo por fecha.

    Implementación MEMORY-optimized usando Polars en modo lazy sin materialización completa.

    Args:
        file_path: Ruta al archivo NDJSON con tweets

    Returns:
        Lista de tuplas (fecha, username) ordenadas por:
        1. Cantidad de tweets (descendente)
        2. Fecha (ascendente) como tie-breaker

    Ejemplo:
        >>> result = q1_memory("tweets.json")
        >>> result[0]
        (datetime.date(2021, 2, 12), 'RanbirS00614606')
    """
    # Crear un LazyFrame a partir del archivo JSON Lines.
    # No se carga el dataset completo en memoria.
    # Solo se seleccionan los campos estrictamente necesarios:
    # - date_only: fecha truncada a nivel día
    # - username: nombre de usuario del autor del tweet
    lazy_df = (
        pl.scan_ndjson(file_path)
        .select([
            pl.col("date").str.slice(0, 10).alias("date_only"),
            pl.col("user").struct.field("username").alias("username")
        ])
        # Filtrar registros inválidos de forma explícita
        .filter(
            pl.col("username").is_not_null() &
            pl.col("date_only").is_not_null()
        )
    )

    # Primera pasada sobre el dataset (streaming):
    # Se agrupa por fecha y se cuentan los tweets por día.
    # Se ordena para obtener el top 10 de fechas más activas.
    # El collect() materializa solo este resultado agregado,
    # no el DataFrame completo.
    top_dates = (
        lazy_df
        .group_by("date_only")
        .agg(pl.len().alias("tweet_count"))
        .sort(["tweet_count", "date_only"], descending=[True, False])
        .head(10)
        .collect()
    )

    results = []

    # Para cada una de las fechas top, se ejecuta una pasada adicional
    # sobre el dataset para encontrar el usuario más activo ese día.
    for row in top_dates.iter_rows(named=True):
        date_str = row["date_only"]

        # Filtrar por fecha específica y agrupar por usuario.
        # Cada collect() ejecuta un scan independiente del archivo,
        # priorizando bajo uso de memoria sobre velocidad.
        top_user = (
            lazy_df
            .filter(pl.col("date_only") == date_str)
            .group_by("username")
            .agg(pl.len().alias("user_tweet_count"))
            .sort(["user_tweet_count", "username"], descending=[True, False])
            .head(1)
            .collect()
        )

        # Extraer el username ganador
        username = top_user["username"][0]

        # Convertir la fecha de string a datetime.date
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()

        # Agregar el resultado final
        results.append((date_obj, username))

    # Retornar la lista de resultados en el formato solicitado
    return results
