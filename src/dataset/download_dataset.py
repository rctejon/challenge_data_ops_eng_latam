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

# Add src directory to path to import common module
sys.path.insert(0, str(Path(__file__).parent.parent))
from common import Colors


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
        print(f"{Colors.RED}Error: gdown is not installed{Colors.RESET}")
        print(f"{Colors.CYAN}Install dependencies with: pip install -r requirements.txt{Colors.RESET}")
        return False

    try:
        # Construir URL de descarga directa de Google Drive
        url = f"https://drive.google.com/uc?id={file_id}"

        if not quiet:
            print(f"Downloading dataset from Google Drive...")
            print(f"  Destination: {output_path}")

        # Descargar archivo
        gdown.download(url, str(output_path), quiet=quiet)

        return True

    except Exception as e:
        print(f"{Colors.RED}Error during download: {e}{Colors.RESET}")
        return False


def extract_if_zip(file_path: Path) -> Path:
    """
    Extrae el archivo si es un ZIP y retorna la ruta del archivo extraído.

    Args:
        file_path: Ruta al archivo descargado

    Returns:
        Ruta al archivo JSON (extraído si era ZIP, original si no)
    """
    import zipfile

    # Verificar si es un archivo ZIP
    if not zipfile.is_zipfile(file_path):
        return file_path

    print(f"  {Colors.CYAN}Detected ZIP archive, extracting...{Colors.RESET}")

    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            # Listar archivos en el ZIP
            file_list = zip_ref.namelist()

            if len(file_list) == 0:
                print(f"  {Colors.RED}Error: ZIP file is empty{Colors.RESET}")
                return file_path

            # Buscar archivo JSON
            json_file = None
            for name in file_list:
                if name.endswith('.json'):
                    json_file = name
                    break

            if not json_file:
                print(f"  {Colors.YELLOW}Warning: No JSON file found in ZIP{Colors.RESET}")
                # Extraer el primer archivo
                json_file = file_list[0]

            # Extraer el archivo
            output_dir = file_path.parent
            zip_ref.extract(json_file, output_dir)
            extracted_path = output_dir / json_file

            print(f"  {Colors.GREEN}Extracted: {json_file}{Colors.RESET}")

            # Renombrar el ZIP original
            zip_backup = file_path.with_suffix(file_path.suffix + '.zip')
            file_path.rename(zip_backup)
            print(f"  {Colors.CYAN}ZIP archived as: {zip_backup.name}{Colors.RESET}")

            return extracted_path

    except Exception as e:
        print(f"  {Colors.RED}Error extracting ZIP: {e}{Colors.RESET}")
        return file_path


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
        print(f"{Colors.RED}Error: File does not exist at {file_path}{Colors.RESET}")
        return False

    # Verificar que el archivo no está vacío
    file_size = file_path.stat().st_size
    if file_size == 0:
        print(f"{Colors.RED}Error: File is empty{Colors.RESET}")
        return False

    # Mostrar tamaño del archivo
    size_mb = file_size / (1024 * 1024)
    print(f"  {Colors.GREEN}File downloaded: {size_mb:.2f} MB{Colors.RESET}")

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
        print(f"  {Colors.YELLOW}Warning: Could not validate file format{Colors.RESET}")
        print(f"  {Colors.YELLOW}File exists and has content, validation failed{Colors.RESET}")
        print(f"  {Colors.YELLOW}This may be normal if dataset has special characters{Colors.RESET}")
        # No retornar False aquí, solo advertir
    else:
        print(f"  {Colors.GREEN}JSON format validated successfully{Colors.RESET}")

    # Contar líneas aproximadas (opcional, puede ser lento para archivos grandes)
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            line_count = sum(1 for _ in f)
        print(f"  {Colors.CYAN}Lines in file: {line_count:,}{Colors.RESET}")
    except Exception:
        # Si falla, no es crítico - el archivo tiene caracteres especiales
        print(f"  {Colors.YELLOW}Could not count lines (file has special characters){Colors.RESET}")

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
            print(f"Output directory: {output_path.absolute()}")
    except Exception as e:
        print(f"{Colors.RED}Error creating directory {output_path}: {e}{Colors.RESET}")
        return 1

    # Ruta completa del archivo
    file_path = output_path / DATASET_FILENAME

    # Verificar si el archivo ya existe
    if file_path.exists():
        print(f"{Colors.YELLOW}File already exists: {file_path}{Colors.RESET}")
        response = input("  Do you want to download it again? (y/n): ").strip().lower()
        if response not in ['y', 'yes', 's', 'si', 'sí']:
            print("  Operation cancelled.")
            return 0
        print("  Replacing existing file...")

    # Descargar archivo
    if not quiet:
        print("\n" + "="*60)
        print("  Starting dataset download")
        print("="*60 + "\n")

    success = download_from_google_drive(
        file_id=GOOGLE_DRIVE_FILE_ID,
        output_path=file_path,
        quiet=quiet
    )

    if not success:
        print(f"\n{Colors.RED}Download failed. Check your internet connection and try again.{Colors.RESET}")
        return 1

    # Extraer si es ZIP
    if not quiet:
        print("\nChecking file type...")

    extracted_path = extract_if_zip(file_path)

    # Validar descarga
    if not quiet:
        print("\nValidating file...")

    if not validate_download(extracted_path):
        print(f"\n{Colors.RED}File validation failed.{Colors.RESET}")
        print(f"{Colors.YELLOW}The file might be corrupted or incomplete.{Colors.RESET}")
        print(f"{Colors.YELLOW}Try downloading again.{Colors.RESET}")
        return 1

    # Actualizar file_path para el mensaje final
    file_path = extracted_path

    # Éxito
    if not quiet:
        print("\n" + "="*60)
        print(f"{Colors.GREEN}Download completed successfully{Colors.RESET}")
        print("="*60)
        print(f"\nDataset saved at: {file_path.absolute()}")
        print(f"\n{Colors.CYAN}Note: Dataset is NOT tracked in git (already in .gitignore){Colors.RESET}")

    return 0


if __name__ == "__main__":
    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(
        description="Download dataset for LATAM Data Engineer Challenge"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help=f"Directory to save dataset (default: {DEFAULT_OUTPUT_DIR})"
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Minimize output messages"
    )

    args = parser.parse_args()

    # Ejecutar descarga
    exit_code = main(output_dir=args.output_dir, quiet=args.quiet)
    sys.exit(exit_code)
