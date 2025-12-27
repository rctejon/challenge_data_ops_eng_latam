"""
Q2 TIME-Optimized Implementation using Polars

Este módulo contiene la implementación optimizada por velocidad para Q2:
- Encuentra los top 10 emojis más usados en tweets
- Retorna los emojis ordenados por frecuencia (descendente) y alfabéticamente (tie-break)

Estrategia:
- Carga completa en memoria con Polars (scan_ndjson + collect)
- Extracción vectorizada de emojis usando map_elements
- Operaciones de explode + group_by para conteo eficiente
- Garbage collection estratégico para liberar memoria intermedia
- Trade-off: velocidad máxima a costa de mayor uso de RAM

Complejidad:
- Tiempo: O(n) para procesamiento + O(k log k) para sort (k = emojis únicos)
- Espacio: O(n) por DataFrame en memoria + O(m) por lista de emojis (m = total emojis)
"""

from typing import List, Tuple
import polars as pl
import emoji
import gc


def q2_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Retorna los top 10 emojis más usados en tweets.

    Implementación TIME-optimized usando Polars con carga completa en memoria.

    Args:
        file_path: Ruta al archivo NDJSON con tweets

    Returns:
        Lista de tuplas (emoji, count) ordenadas por:
        1. Cantidad de ocurrencias (descendente)
        2. Emoji alfabéticamente (ascendente) como tie-breaker

    Ejemplo:
        >>> result = q2_time("tweets.json")
        >>> result[0]
        ('🙏', 5049)
    """
    # Leer el archivo JSON y extraer emojis en una sola pasada
    df = (
        pl.scan_ndjson(file_path)
        .select([pl.col("content")])
        .filter(pl.col("content").is_not_null())
        .collect()
        # Extraer lista de emojis de cada tweet usando map_elements
        .with_columns(
            pl.col("content").map_elements(
                lambda x: [e['emoji'] for e in emoji.emoji_list(x)] if x else [],
                return_dtype=pl.List(pl.Utf8)
            ).alias("emoji_list")
        )
        # Drop content column - ya no la necesitamos, liberar memoria
        .drop("content")
    )

    # Explotar la lista de emojis para tener un emoji por fila
    # Luego agrupar y contar
    emoji_counts = (
        df
        .explode("emoji_list")
        .filter(pl.col("emoji_list").is_not_null())
        .group_by("emoji_list")
        .agg(pl.len().alias("count"))
        # Ordenamiento determinístico:
        # 1. Por conteo descendente
        # 2. Por emoji ascendente (tie-break alfabético)
        .sort(["count", "emoji_list"], descending=[True, False])
        .head(10)
    )

    # Liberar memoria del DataFrame intermedio antes de convertir resultados
    del df
    gc.collect()

    # Convertir a lista de tuplas (resultado final pequeño)
    top_10 = [
        (row["emoji_list"], row["count"])
        for row in emoji_counts.iter_rows(named=True)
    ]

    # Liberar memoria del DataFrame de conteos
    del emoji_counts
    gc.collect()

    return top_10
