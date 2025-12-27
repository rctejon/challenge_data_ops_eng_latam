"""
Q2 MEMORY Implementation Runner

Este archivo ejecuta la implementación MEMORY-optimized de Q2 con profiling completo.

Ejecutar:
    python src/q2/q2_memory_impl.py

Genera:
    - q2_memory_polars.prof: Profiling de tiempo (cProfile)
    - q2_memory_polars_mem.bin: Profiling de memoria (memray)

Arquitectura:
    - Importa la función pura desde q2_memory.py
    - Ejecuta benchmarking de tiempo y memoria
    - Guarda resultados para análisis posterior
"""

import sys
import time
import cProfile
import pstats
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from common import Colors

# Import the implementation
from q2.q2_memory import q2_memory


# Dataset path
DATASET_PATH = "data/raw/farmers-protest-tweets-2021-2-4.json"
PROFILE_OUTPUT = "q2_memory_polars.prof"
MEMRAY_OUTPUT = "q2_memory_polars_mem.bin"


def verify_dataset() -> Path:
    """Verifica que el dataset existe y retorna su Path."""
    dataset_path = Path(DATASET_PATH)

    if not dataset_path.exists():
        print(f"{Colors.RED}Error: Dataset not found at {DATASET_PATH}{Colors.RESET}")
        print(f"{Colors.CYAN}Run first: python src/dataset/download_dataset.py{Colors.RESET}")
        sys.exit(1)

    file_size_mb = dataset_path.stat().st_size / (1024 * 1024)
    print(f"{Colors.GREEN}Dataset found: {file_size_mb:.2f} MB{Colors.RESET}")

    return dataset_path


def run_and_display_results(dataset_path: Path) -> None:
    """Ejecuta la función y muestra los resultados."""
    print(f"\n{Colors.BOLD}Executing q2_memory()...{Colors.RESET}")
    print("=" * 80)

    start = time.time()
    result = q2_memory(str(dataset_path))
    end = time.time()

    print(f"\n{Colors.GREEN}Results - Top 10 Most Used Emojis:{Colors.RESET}")
    print("-" * 80)
    for i, (emoji_char, count) in enumerate(result, 1):
        print(f"  {i:2d}. {emoji_char} -> {count:,} occurrences")

    print(f"\n{Colors.CYAN}Execution time: {end - start:.3f}s{Colors.RESET}")
    print("=" * 80)


def profile_time(dataset_path: Path) -> None:
    """Ejecuta profiling de tiempo con cProfile."""
    print(f"\n{Colors.BOLD}Running cProfile...{Colors.RESET}")
    print("=" * 80)

    profiler = cProfile.Profile()
    profiler.enable()

    _ = q2_memory(str(dataset_path))

    profiler.disable()

    # Save to file
    profiler.dump_stats(PROFILE_OUTPUT)

    # Print summary
    stats = pstats.Stats(profiler)
    stats.strip_dirs()
    stats.sort_stats('cumulative')

    print(f"\n{Colors.GREEN}Top 10 functions by cumulative time:{Colors.RESET}")
    print("-" * 80)
    stats.print_stats(10)

    print(f"\n{Colors.CYAN}Profile saved to: {PROFILE_OUTPUT}{Colors.RESET}")
    print(f"{Colors.YELLOW}Analyze with: python -m pstats {PROFILE_OUTPUT}{Colors.RESET}")
    print("=" * 80)


def profile_memory(dataset_path: Path) -> None:
    """Ejecuta profiling de memoria con memray."""
    print(f"\n{Colors.BOLD}Running memray...{Colors.RESET}")
    print("=" * 80)

    try:
        import memray
    except ImportError:
        print(f"{Colors.YELLOW}Warning: memray not installed{Colors.RESET}")
        print(f"{Colors.CYAN}Install with: pip install memray{Colors.RESET}")
        print(f"{Colors.YELLOW}Skipping memory profiling...{Colors.RESET}")
        print("=" * 80)
        return

    # Run with memray tracker
    with memray.Tracker(MEMRAY_OUTPUT):
        _ = q2_memory(str(dataset_path))

    print(f"\n{Colors.GREEN}Memory profile saved to: {MEMRAY_OUTPUT}{Colors.RESET}")
    print(f"{Colors.YELLOW}Generate flamegraph with:{Colors.RESET}")
    print(f"  memray flamegraph {MEMRAY_OUTPUT}")
    print(f"{Colors.YELLOW}Generate table with:{Colors.RESET}")
    print(f"  memray table {MEMRAY_OUTPUT}")
    print(f"{Colors.YELLOW}Generate stats with:{Colors.RESET}")
    print(f"  memray stats {MEMRAY_OUTPUT}")
    print("=" * 80)


def main():
    """Función principal que ejecuta todo el pipeline."""
    print("\n" + "=" * 80)
    print(f"{Colors.BOLD}{Colors.CYAN}Q2 MEMORY-Optimized Implementation (Polars){Colors.RESET}")
    print("=" * 80)

    # 1. Verify dataset
    dataset_path = verify_dataset()

    # 2. Run and display results
    run_and_display_results(dataset_path)

    # 3. Profile time
    profile_time(dataset_path)

    # 4. Profile memory
    profile_memory(dataset_path)

    # 5. Summary
    print("\n" + "=" * 80)
    print(f"{Colors.BOLD}{Colors.GREEN}Profiling Complete{Colors.RESET}")
    print("=" * 80)
    print(f"\n{Colors.CYAN}Generated files:{Colors.RESET}")
    print(f"  - {PROFILE_OUTPUT} (cProfile)")
    if Path(MEMRAY_OUTPUT).exists():
        print(f"  - {MEMRAY_OUTPUT} (memray)")
    print()


if __name__ == "__main__":
    main()
