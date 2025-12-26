"""
Dataset Profiling Script - LATAM Data Engineer Challenge

Este script realiza un análisis exhaustivo del dataset de tweets para:
1. Validar calidad de datos
2. Identificar casos borde
3. Guiar decisiones de diseño para q1, q2, q3

El profiling se hace de manera eficiente usando streaming para evitar
cargar el dataset completo en memoria.

Uso:
    python dataset_profile.py [ruta_al_dataset]

Si no se proporciona ruta, usa: data/raw/farmers-protest-tweets-2021-2-4.json
"""

import sys
import time
from pathlib import Path
from typing import Dict, Any, Optional
from collections import defaultdict, Counter
from datetime import datetime

# Add src directory to path to import common module
sys.path.insert(0, str(Path(__file__).parent.parent))
from common import Colors


def profile_dataset(file_path: str, sample_size: Optional[int] = None) -> Dict[str, Any]:
    """
    Analiza el dataset de tweets y retorna métricas clave.

    Args:
        file_path: Ruta al archivo JSON Lines del dataset
        sample_size: Si se especifica, solo analiza las primeras N líneas

    Returns:
        Diccionario con estadísticas del dataset
    """
    try:
        import orjson
    except ImportError:
        print(f"{Colors.RED}Error: orjson no está instalado{Colors.RESET}")
        print(f"{Colors.CYAN}Instala con: pip install -r requirements.txt{Colors.RESET}")
        return {}

    stats = {
        'total_lines': 0,
        'valid_lines': 0,
        'invalid_lines': 0,
        'non_dict_lines': 0,
        'parse_errors': [],
        'missing_fields': defaultdict(int),
        'field_types': defaultdict(Counter),
        'date_formats': Counter(),
        'content_lengths': [],
        'mentions_stats': {
            'with_mentions': 0,
            'without_mentions': 0,
            'null_mentions': 0,
            'empty_list_mentions': 0,
        },
        'processing_time': 0,
    }

    start_time = time.time()

    print(f"Analyzing dataset: {file_path}")
    print("=" * 60 + "\n")

    try:
        with open(file_path, 'rb') as f:
            for line_num, line in enumerate(f, 1):
                stats['total_lines'] += 1

                # Limitar a muestra si se especificó
                if sample_size and line_num > sample_size:
                    break

                # Mostrar progreso cada 10k líneas
                if line_num % 10000 == 0:
                    elapsed = time.time() - start_time
                    throughput = line_num / elapsed if elapsed > 0 else 0
                    print(f"   Processed: {line_num:,} lines ({throughput:.0f} lines/sec)", end='\r')

                # Intentar parsear la línea
                try:
                    tweet = orjson.loads(line)

                    # Verificar que sea un diccionario (no un número u otro tipo)
                    if not isinstance(tweet, dict):
                        stats['non_dict_lines'] += 1
                        stats['invalid_lines'] += 1
                        if len(stats['parse_errors']) < 5:
                            stats['parse_errors'].append({
                                'line': line_num,
                                'error': f'Not a dictionary: {type(tweet).__name__}',
                                'sample': str(tweet)[:100]
                            })
                        continue

                    stats['valid_lines'] += 1

                    # Analizar campos clave
                    analyze_tweet_fields(tweet, stats)

                except orjson.JSONDecodeError as e:
                    stats['invalid_lines'] += 1
                    if len(stats['parse_errors']) < 5:  # Guardar solo los primeros 5 errores
                        stats['parse_errors'].append({
                            'line': line_num,
                            'error': str(e),
                            'sample': line[:100].decode('utf-8', errors='ignore')
                        })

    except FileNotFoundError:
        print(f"{Colors.RED}Error: File not found: {file_path}{Colors.RESET}")
        print(f"{Colors.CYAN}Run first: python src/dataset/download_dataset.py{Colors.RESET}")
        return {}

    stats['processing_time'] = time.time() - start_time

    # Calcular métricas finales
    calculate_final_metrics(stats)

    return stats


def analyze_tweet_fields(tweet: Dict[str, Any], stats: Dict[str, Any]) -> None:
    """
    Analiza los campos de un tweet individual y actualiza estadísticas.
    """
    # Verificar presencia de campos críticos
    critical_fields = ['date', 'content', 'user', 'mentionedUsers']

    for field in critical_fields:
        if field not in tweet or tweet[field] is None:
            stats['missing_fields'][field] += 1

        # Registrar tipo de dato
        if field in tweet:
            field_type = type(tweet[field]).__name__
            stats['field_types'][field][field_type] += 1

    # Analizar campo date
    if 'date' in tweet and tweet['date']:
        try:
            # Intentar detectar formato
            date_str = str(tweet['date'])
            if 'T' in date_str:
                stats['date_formats']['ISO-8601'] += 1
            elif '/' in date_str:
                stats['date_formats']['MM/DD/YYYY'] += 1
            else:
                stats['date_formats']['OTHER'] += 1
        except Exception:
            pass

    # Analizar campo content
    if 'content' in tweet and tweet['content']:
        content_len = len(str(tweet['content']))
        stats['content_lengths'].append(content_len)

    # Analizar campo user.username
    if 'user' in tweet and tweet['user']:
        if isinstance(tweet['user'], dict):
            if 'username' not in tweet['user'] or tweet['user']['username'] is None:
                stats['missing_fields']['user.username'] += 1

    # Analizar menciones
    if 'mentionedUsers' not in tweet or tweet['mentionedUsers'] is None:
        stats['mentions_stats']['null_mentions'] += 1
    elif isinstance(tweet['mentionedUsers'], list):
        if len(tweet['mentionedUsers']) == 0:
            stats['mentions_stats']['empty_list_mentions'] += 1
            stats['mentions_stats']['without_mentions'] += 1
        else:
            stats['mentions_stats']['with_mentions'] += 1
    else:
        stats['mentions_stats']['without_mentions'] += 1


def calculate_final_metrics(stats: Dict[str, Any]) -> None:
    """
    Calcula métricas finales derivadas.
    """
    # Calcular porcentajes de líneas inválidas
    if stats['total_lines'] > 0:
        stats['invalid_percentage'] = (stats['invalid_lines'] / stats['total_lines']) * 100
    else:
        stats['invalid_percentage'] = 0

    # Calcular estadísticas de longitud de content
    if stats['content_lengths']:
        stats['content_lengths'].sort()
        n = len(stats['content_lengths'])
        stats['content_stats'] = {
            'min': stats['content_lengths'][0],
            'max': stats['content_lengths'][-1],
            'p50': stats['content_lengths'][n // 2],
            'p95': stats['content_lengths'][int(n * 0.95)],
            'p99': stats['content_lengths'][int(n * 0.99)],
        }
    else:
        stats['content_stats'] = {}

    # Calcular throughput
    if stats['processing_time'] > 0:
        stats['throughput'] = stats['total_lines'] / stats['processing_time']
    else:
        stats['throughput'] = 0


def print_report(stats: Dict[str, Any]) -> None:
    """
    Imprime un reporte legible de las estadísticas del dataset.
    """
    print("\n" + "="*60)
    print("  DATASET PROFILING REPORT")
    print("="*60 + "\n")

    # Sección 1: Validación básica
    print(f"{Colors.BOLD}BASIC VALIDATION{Colors.RESET}")
    print(f"  Total lines: {stats['total_lines']:,}")
    print(f"  {Colors.GREEN}Valid lines: {stats['valid_lines']:,}{Colors.RESET}")
    print(f"  {Colors.RED}Invalid lines: {stats['invalid_lines']:,} ({stats['invalid_percentage']:.2f}%){Colors.RESET}")
    if stats.get('non_dict_lines', 0) > 0:
        print(f"  {Colors.YELLOW}  - Non-dictionary JSON: {stats['non_dict_lines']:,}{Colors.RESET}")
    print()

    # Sección 2: Calidad de datos
    print(f"{Colors.BOLD}DATA QUALITY - Missing Fields{Colors.RESET}")
    if stats['missing_fields']:
        for field, count in sorted(stats['missing_fields'].items(), key=lambda x: x[1], reverse=True):
            percentage = (count / stats['valid_lines']) * 100 if stats['valid_lines'] > 0 else 0
            color = Colors.RED if percentage > 1 else Colors.YELLOW
            print(f"  {color}{field}: {count:,} ({percentage:.2f}%){Colors.RESET}")
    else:
        print(f"  {Colors.GREEN}No missing fields detected{Colors.RESET}")
    print()

    # Sección 3: Formato de fechas
    print(f"{Colors.BOLD}DATE FORMATS{Colors.RESET}")
    if stats['date_formats']:
        for fmt, count in stats['date_formats'].most_common():
            percentage = (count / stats['valid_lines']) * 100 if stats['valid_lines'] > 0 else 0
            print(f"  {Colors.CYAN}{fmt}: {count:,} ({percentage:.2f}%){Colors.RESET}")
    print()

    # Sección 4: Estadísticas de contenido
    print(f"{Colors.BOLD}CONTENT STATISTICS{Colors.RESET}")
    if stats['content_stats']:
        cs = stats['content_stats']
        print(f"  Min length: {cs['min']:,} chars")
        print(f"  P50 (median): {cs['p50']:,} chars")
        print(f"  P95: {cs['p95']:,} chars")
        print(f"  P99: {cs['p99']:,} chars")
        print(f"  Max length: {cs['max']:,} chars")
    print()

    # Sección 5: Menciones
    print(f"{Colors.BOLD}MENTIONS ANALYSIS{Colors.RESET}")
    ms = stats['mentions_stats']
    total_valid = stats['valid_lines']
    if total_valid > 0:
        print(f"  {Colors.GREEN}With mentions: {ms['with_mentions']:,} ({ms['with_mentions']/total_valid*100:.2f}%){Colors.RESET}")
        print(f"  {Colors.YELLOW}Without mentions: {ms['without_mentions']:,} ({ms['without_mentions']/total_valid*100:.2f}%){Colors.RESET}")
        print(f"  {Colors.YELLOW}Null field: {ms['null_mentions']:,} ({ms['null_mentions']/total_valid*100:.2f}%){Colors.RESET}")
        print(f"  {Colors.YELLOW}Empty list: {ms['empty_list_mentions']:,} ({ms['empty_list_mentions']/total_valid*100:.2f}%){Colors.RESET}")
    print()

    # Sección 6: Performance
    print(f"{Colors.BOLD}PERFORMANCE{Colors.RESET}")
    print(f"  Processing time: {stats['processing_time']:.2f} seconds")
    print(f"  Throughput: {stats['throughput']:.0f} lines/second")
    print()

    # Sección 7: Errores de parseo (si existen)
    if stats['parse_errors']:
        print(f"{Colors.YELLOW}PARSE ERRORS (first 5){Colors.RESET}")
        for error in stats['parse_errors']:
            print(f"  Line {error['line']}: {error['error']}")
            print(f"  Sample: {error['sample']}")
        print()

    print("="*60)
    print(f"{Colors.GREEN}Profiling completed{Colors.RESET}")
    print("="*60)


def main():
    """
    Función principal del script.
    """
    # Determinar ruta del dataset
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = "data/raw/farmers-protest-tweets-2021-2-4.json"

    file_path = Path(file_path)

    if not file_path.exists():
        print(f"{Colors.RED}Error: File not found: {file_path}{Colors.RESET}")
        print(f"\n{Colors.CYAN}Download the dataset first:{Colors.RESET}")
        print(f"  python src/dataset/download_dataset.py")
        sys.exit(1)

    # Ejecutar profiling
    stats = profile_dataset(str(file_path))

    if stats:
        # Imprimir reporte
        print_report(stats)

        # Imprimir decisiones técnicas recomendadas
        print("\n" + "="*60)
        print("  RECOMMENDED TECHNICAL DECISIONS")
        print("="*60 + "\n")

        print(f"{Colors.BOLD}For Q1 (Top dates and users):{Colors.RESET}")
        print("  - Parse 'date' field with robust error handling")
        print("  - Extract 'user.username' checking existence first")
        print(f"  - Handle {stats['missing_fields'].get('date', 0):,} tweets without date")
        print()

        print(f"{Colors.BOLD}For Q2 (Top emojis):{Colors.RESET}")
        print("  - Analyze 'content' field using emoji library")
        print(f"  - Consider {stats['missing_fields'].get('content', 0):,} tweets without content")
        print("  - Handle compound emojis correctly")
        print()

        print(f"{Colors.BOLD}For Q3 (Top influential users by mentions):{Colors.RESET}")
        ms = stats['mentions_stats']
        print(f"  - Use 'mentionedUsers' field ({ms['with_mentions']:,} tweets with mentions)")
        print(f"  - Handle null cases ({ms['null_mentions']:,} tweets)")
        print(f"  - Handle empty lists ({ms['empty_list_mentions']:,} tweets)")
        print()

        print(f"{Colors.BOLD}General approach:{Colors.RESET}")
        if stats['invalid_percentage'] > 0:
            print(f"  {Colors.YELLOW}! Skip {stats['invalid_lines']:,} invalid lines ({stats['invalid_percentage']:.2f}%){Colors.RESET}")
        print(f"  {Colors.GREEN}+ Use orjson for efficient parsing{Colors.RESET}")
        print(f"  {Colors.GREEN}+ Implement streaming for memory-optimized approach{Colors.RESET}")
        print()


if __name__ == "__main__":
    main()
