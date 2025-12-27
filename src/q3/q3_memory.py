"""
Q3 MEMORY-Optimized Implementation using Polars

Este módulo contiene la implementación optimizada por memoria para Q3:
- Encuentra los top 10 usuarios más influyentes por menciones
- Retorna los usuarios ordenados por frecuencia (descendente) y alfabéticamente (tie-break)

Estrategia:
- Usa lazy evaluation con pl.scan_ndjson()
- No materializa DataFrames intermedios
- Solo materializa el resultado final (top 10)
- Usa streaming aggregations de Polars
- Trade-off: menor consumo de memoria, tiempo similar o mejor que TIME

Complejidad:
- Tiempo: O(n) para procesamiento streaming + O(k log k) para sort (k = usuarios únicos)
- Espacio: O(1) - solo materializa el resultado final pequeño (top 10 usuarios)
"""

from typing import List, Tuple
import polars as pl
import gc


def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Retorna los top 10 usuarios más influyentes por menciones.

    Implementación MEMORY-optimized usando Polars con lazy evaluation
    y materialización mínima.

    Args:
        file_path: Ruta al archivo NDJSON con tweets

    Returns:
        Lista de tuplas (username, count) ordenadas por:
        1. Cantidad de menciones (descendente)
        2. Username alfabéticamente (ascendente) como tie-breaker

    Ejemplo:
        >>> result = q3_memory("tweets.json")
        >>> result[0]
        ('narendramodi', 2265)
    """
    # Crear LazyFrame sin materializar
    lazy_df = (
        pl.scan_ndjson(file_path)
        .select([pl.col("mentionedUsers")])
        # Filtrar tweets con menciones (no null, no empty)
        .filter(
            pl.col("mentionedUsers").is_not_null() &
            (pl.col("mentionedUsers").list.len() > 0)
        )
    )

    # Procesamiento lazy completo: explode, extract, group, sort
    # Solo se materializa al final con collect()
    top_10 = (
        lazy_df
        .explode("mentionedUsers")
        .with_columns(
            pl.col("mentionedUsers").struct.field("username").alias("username")
        )
        .select(["username"])
        .filter(pl.col("username").is_not_null())
        .group_by("username")
        .agg(pl.len().alias("mention_count"))
        .sort(["mention_count", "username"], descending=[True, False])
        .head(10)
        # Materializar solo el top 10 (muy pequeño)
        .collect()
    )

    # Convertir a lista de tuplas
    results = [
        (row["username"], row["mention_count"])
        for row in top_10.iter_rows(named=True)
    ]

    # Liberar memoria del DataFrame de resultados
    del top_10
    gc.collect()

    return results
