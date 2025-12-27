"""
Q3 TIME-Optimized Implementation using Polars

Este módulo contiene la implementación optimizada por velocidad para Q3:
- Encuentra los top 10 usuarios más influyentes por menciones
- Retorna los usuarios ordenados por frecuencia (descendente) y alfabéticamente (tie-break)

Estrategia:
- Carga completa en memoria con Polars (scan_ndjson + collect)
- Extracción de menciones desde el campo estructurado mentionedUsers
- Operaciones de explode + struct.field para extracción eficiente
- Group_by para conteo eficiente
- Trade-off: velocidad máxima a costa de mayor uso de RAM

Complejidad:
- Tiempo: O(n) para procesamiento + O(k log k) para sort (k = usuarios únicos)
- Espacio: O(n) por DataFrame en memoria + O(m) por menciones (m = total menciones)
"""

from typing import List, Tuple
import polars as pl
import gc


def q3_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Retorna los top 10 usuarios más influyentes por menciones.

    Implementación TIME-optimized usando Polars con carga completa en memoria.

    Args:
        file_path: Ruta al archivo NDJSON con tweets

    Returns:
        Lista de tuplas (username, count) ordenadas por:
        1. Cantidad de menciones (descendente)
        2. Username alfabéticamente (ascendente) como tie-breaker

    Ejemplo:
        >>> result = q3_time("tweets.json")
        >>> result[0]
        ('narendramodi', 2265)
    """
    # Leer el archivo JSON y extraer solo el campo mentionedUsers
    df = (
        pl.scan_ndjson(file_path)
        .select([pl.col("mentionedUsers")])
        # Filtrar tweets que tienen menciones (no null, no empty list)
        .filter(
            pl.col("mentionedUsers").is_not_null() &
            (pl.col("mentionedUsers").list.len() > 0)
        )
        # Materializar en memoria
        .collect()
    )

    # Explotar la lista de menciones para tener una fila por mención
    # Cada elemento de la lista es un struct {username, displayname, id, ...}
    mentions_df = (
        df
        .explode("mentionedUsers")
        # Extraer el campo username del struct
        .with_columns(
            pl.col("mentionedUsers").struct.field("username").alias("username")
        )
        .select(["username"])
        # Filtrar usernames nulos (por si acaso)
        .filter(pl.col("username").is_not_null())
    )

    # Contar menciones por usuario y obtener top 10
    # Ordenamiento determinístico:
    # 1. Por conteo de menciones (descendente)
    # 2. Por username (ascendente) para tie-breaks
    top_10 = (
        mentions_df
        .group_by("username")
        .agg(pl.len().alias("mention_count"))
        .sort(["mention_count", "username"], descending=[True, False])
        .head(10)
    )

    # Liberar memoria del DataFrame intermedio antes de convertir resultados
    del df
    del mentions_df
    gc.collect()

    # Convertir a lista de tuplas (username, count)
    results = [
        (row["username"], row["mention_count"])
        for row in top_10.iter_rows(named=True)
    ]

    # Liberar memoria del DataFrame de conteos
    del top_10
    gc.collect()

    return results
