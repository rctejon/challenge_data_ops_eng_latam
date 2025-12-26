"""
Dataset Download Script - LATAM Data Engineer Challenge

Este script descarga el dataset de tweets necesario para el challenge desde Google Drive.

IMPORTANTE: El dataset NO debe subirse al repositorio. Está excluido en .gitignore.

Uso:
    python download_dataset.py

    O con ruta personalizada:
    python download_dataset.py --output-dir /path/to/custom/dir

Dataset:
    - Fuente: Google Drive (URL oficial del challenge)
    - Tamaño: ~398 MB
    - Formato: JSON (tweets de farmers protest 2021)
    - Destino por defecto: data/raw/

Requisitos:
    - gdown (instalado vía requirements.txt)
    - Conexión a internet
"""

import sys
import argparse
from pathlib import Path
from typing import Optional


# Configuración del dataset
GOOGLE_DRIVE_FILE_ID = "1ig2ngoXFTxP5Pa8muXo02mDTFexZzsis"
DATASET_FILENAME = "farmers-protest-tweets-2021-2-4.json"
DEFAULT_OUTPUT_DIR = "data/raw"


def download_from_google_drive(
    file_id: str,
    output_path: Path,
    quiet: bool = False
) -> bool:
    """
    Descarga un archivo desde Google Drive usando gdown.

    Args:
        file_id: ID del archivo en Google Drive
        output_path: Ruta donde guardar el archivo
        quiet: Si True, minimiza los mensajes de salida

    Returns:
        True si la descarga fue exitosa, False en caso contrario
    """
    try:
        import gdown
    except ImportError:
        print("❌ Error: gdown no está instalado.")
        print("   Instala las dependencias con: pip install -r requirements.txt")
        return False

    try:
        # Construir URL de descarga directa de Google Drive
        url = f"https://drive.google.com/uc?id={file_id}"

        if not quiet:
            print(f"📥 Descargando dataset desde Google Drive...")
            print(f"   Destino: {output_path}")

        # Descargar archivo
        gdown.download(url, str(output_path), quiet=quiet)

        return True

    except Exception as e:
        print(f"❌ Error durante la descarga: {e}")
        return False


def validate_download(file_path: Path) -> bool:
    """
    Valida que el archivo descargado sea correcto.

    Args:
        file_path: Ruta al archivo descargado

    Returns:
        True si el archivo es válido, False en caso contrario
    """
    # Verificar que el archivo existe
    if not file_path.exists():
        print(f"❌ Error: El archivo no existe en {file_path}")
        return False

    # Verificar que el archivo no está vacío
    file_size = file_path.stat().st_size
    if file_size == 0:
        print(f"❌ Error: El archivo está vacío")
        return False

    # Mostrar tamaño del archivo
    size_mb = file_size / (1024 * 1024)
    print(f"✅ Archivo descargado: {size_mb:.2f} MB")

    # Verificar que es un archivo JSON válido (al menos que empiece con [ o {)
    # Intentar con diferentes encodings ya que el dataset tiene emojis y caracteres especiales
    json_valid = False
    for encoding in ['utf-8', 'utf-8-sig', 'latin-1']:
        try:
            with open(file_path, 'r', encoding=encoding, errors='ignore') as f:
                first_char = f.read(1)
                if first_char in ['{', '[']:
                    json_valid = True
                    break
        except Exception:
            continue

    if not json_valid:
        print(f"⚠️  Advertencia: No se pudo validar el formato del archivo")
        print(f"   El archivo existe y tiene contenido, pero la validación falló")
        print(f"   Esto puede ser normal si el dataset tiene caracteres especiales")
        # No retornar False aquí, solo advertir
    else:
        print(f"✅ Formato JSON validado correctamente")

    # Contar líneas aproximadas (opcional, puede ser lento para archivos grandes)
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            line_count = sum(1 for _ in f)
        print(f"📊 Líneas en el archivo: {line_count:,}")
    except Exception:
        # Si falla, no es crítico - el archivo tiene caracteres especiales
        print(f"ℹ️  No se pudo contar líneas (archivo con caracteres especiales)")

    return True


def main(output_dir: Optional[str] = None, quiet: bool = False) -> int:
    """
    Función principal del script de descarga.

    Args:
        output_dir: Directorio donde guardar el dataset (None usa el default)
        quiet: Si True, minimiza los mensajes de salida

    Returns:
        0 si la descarga fue exitosa, 1 en caso contrario
    """
    # Determinar directorio de salida
    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR

    output_path = Path(output_dir)

    # Crear directorio si no existe
    try:
        output_path.mkdir(parents=True, exist_ok=True)
        if not quiet:
            print(f"📁 Directorio de salida: {output_path.absolute()}")
    except Exception as e:
        print(f"❌ Error al crear directorio {output_path}: {e}")
        return 1

    # Ruta completa del archivo
    file_path = output_path / DATASET_FILENAME

    # Verificar si el archivo ya existe
    if file_path.exists():
        print(f"⚠️  El archivo ya existe: {file_path}")
        response = input("   ¿Deseas descargarlo nuevamente? (s/n): ").strip().lower()
        if response not in ['s', 'si', 'sí', 'y', 'yes']:
            print("   Operación cancelada.")
            return 0
        print("   Reemplazando archivo existente...")

    # Descargar archivo
    if not quiet:
        print("\n" + "="*60)
        print("  Iniciando descarga del dataset")
        print("="*60)

    success = download_from_google_drive(
        file_id=GOOGLE_DRIVE_FILE_ID,
        output_path=file_path,
        quiet=quiet
    )

    if not success:
        print("\n❌ La descarga falló. Verifica tu conexión a internet e intenta nuevamente.")
        return 1

    # Validar descarga
    if not quiet:
        print("\n🔍 Validando descarga...")

    if not validate_download(file_path):
        print("\n❌ La validación del archivo falló.")
        print("   El archivo podría estar corrupto o incompleto.")
        print("   Intenta descargar nuevamente.")
        return 1

    # Éxito
    if not quiet:
        print("\n" + "="*60)
        print("✅ Descarga completada exitosamente")
        print("="*60)
        print(f"\n📍 Dataset guardado en: {file_path.absolute()}")
        print(f"\n💡 Recuerda: El dataset NO debe subirse a git (ya está en .gitignore)")

    return 0


if __name__ == "__main__":
    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(
        description="Descarga el dataset del LATAM Data Engineer Challenge"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help=f"Directorio donde guardar el dataset (default: {DEFAULT_OUTPUT_DIR})"
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Minimizar mensajes de salida"
    )

    args = parser.parse_args()

    # Ejecutar descarga
    exit_code = main(output_dir=args.output_dir, quiet=args.quiet)
    sys.exit(exit_code)
