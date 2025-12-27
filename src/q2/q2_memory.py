"""
Q2 MEMORY-Optimized Implementation using Polars

Este módulo contiene la implementación optimizada por memoria para Q2:
- Encuentra los top 10 emojis más usados en tweets
- Retorna los emojis ordenados por frecuencia (descendente) y alfabéticamente (tie-break)

Estrategia:
- Usa lazy evaluation con pl.scan_ndjson()
- Materializa solo el campo "content" (no todo el JSON)
- Procesa row-by-row extrayendo emojis con emoji.emoji_list()
- Usa Counter incremental (estructura muy eficiente en memoria)
- Libera memoria explícitamente con del + gc.collect()
- Trade-off: mayor tiempo de ejecución a cambio de menor uso de RAM

Complejidad:
- Tiempo: O(n) para procesamiento row-by-row + O(k log k) para sort (k = emojis únicos)
- Espacio: O(1) - solo almacena content column + Counter de emojis únicos

Limitación:
- emoji.emoji_list() requiere procesamiento row-by-row en Python,
  lo que limita las optimizaciones de streaming puro de Polars.
"""

from typing import List, Tuple
from collections import Counter
import polars as pl
import emoji
import gc


def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Retorna los top 10 emojis más usados en tweets.

    Implementación MEMORY-optimized usando Polars con lazy evaluation
    y procesamiento row-by-row.

    Args:
        file_path: Ruta al archivo NDJSON con tweets

    Returns:
        Lista de tuplas (emoji, count) ordenadas por:
        1. Cantidad de ocurrencias (descendente)
        2. Emoji alfabéticamente (ascendente) como tie-breaker

    Ejemplo:
        >>> result = q2_memory("tweets.json")
        >>> result[0]
        ('🙏', 5049)
    """
    # Crear LazyFrame sin materializar el dataset completo
    # Solo seleccionar el campo "content" que es el que necesitamos
    lazy_df = (
        pl.scan_ndjson(file_path)
        .select([pl.col("content")])
        .filter(pl.col("content").is_not_null())
    )

    # Contador para almacenar emojis (muy eficiente en memoria)
    # Solo almacena emojis únicos con su conteo, no todas las filas
    emoji_counter = Counter()

    # Materializar solo el campo content (no todo el JSON)
    # Esto sigue siendo más eficiente que Q2 TIME que materializa
    # todo el dataset y luego aplica transformaciones vectorizadas
    df = lazy_df.collect()

    # Extraer emojis row-by-row
    # Limitación: emoji.emoji_list() requiere procesamiento en Python
    # No se puede vectorizar completamente con Polars
    for row in df.iter_rows(named=True):
        content = row["content"]
        if content:
            emojis_found = emoji.emoji_list(content)
            for emoji_data in emojis_found:
                emoji_char = emoji_data['emoji']
                emoji_counter[emoji_char] += 1

    # Liberar DataFrame inmediatamente después de procesar
    del df
    gc.collect()

    # Obtener top 10 con ordenamiento determinístico:
    # 1. Por conteo descendente (-x[1])
    # 2. Por emoji ascendente (x[0]) como tie-breaker
    top_10 = sorted(
        emoji_counter.items(),
        key=lambda x: (-x[1], x[0])
    )[:10]

    # Liberar Counter (opcional, Python lo haría automáticamente)
    del emoji_counter
    gc.collect()

    return top_10
