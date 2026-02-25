"""
Parser para sanciones de la Procuraduría (PACO_DISC) y
la Fiscalía (PACO_PENAL), ambas en formato CSV (una en ZIP)
"""

import csv
import io
import zipfile
from typing import List, Dict, Optional

from pipeline.utils import build_canonical, get_logger



logger = get_logger("fuentes.paco")


def _find_column(headers:List[str], candidates:List[str]) -> Optional[int]:
    """
    Encuentra el índice de la primera columna que coincide (case-insensitive, parcial)
    con alguno de los candidatos
    """
    headers_norm = [h.upper().strip() for h in headers]
    for cand in candidates:
        cand_upper = cand.upper()
        for i, h in enumerate(headers_norm):
            if cand_upper in h or h in cand_upper:
                return i
    return None


def _map_columns(headers:List[str]) -> Dict[str, Optional[int]]:
    """
    Mapea nombres de columnas semánticas a índices reales usando heurística
    """
    return {
        "nombres": _find_column(headers,
            ["NOMBRE", "NOMBRES", "PRIMER_NOMBRE", "FIRST_NAME"]),
        "apellidos": _find_column(headers,
            ["APELLIDO", "APELLIDOS", "PRIMER_APELLIDO"]),
        "cedula": _find_column(headers,
            ["CEDULA", "NRO_CEDULA", "NUMERO_CEDULA", "DOCUMENTO", "NRO_DOC", "IDENTIFICACION"]),
        "tipo_sancion": _find_column(headers,
            ["TIPO_SANCION", "SANCION", "DELITO", "CARGO", "FALTA", "DESCRIPCION_SANCION"]),
        "fecha_sancion": _find_column(headers,
            ["FECHA_SANCION", "FECHA_CONDENA", "FECHA_EJECUTORIA", "FECHA_INICIO"]),
        "fecha_vencimiento": _find_column(headers,
            ["FECHA_VENCIMIENTO", "FECHA_FIN", "FECHA_HASTA", "FECHA_TERMINACION"]),
        "estado": _find_column(headers,
            ["ESTADO", "VIGENTE", "ACTIVO", "STATUS"]),
    }


def _row_to_record(
        row:List[str],
        col_map:Dict[str, Optional[int]],
        fuente:str,
        row_idx:int
    ) -> Optional[Dict]:
    def get(field) -> str:
        idx = col_map.get(field)
        if idx is not None and idx < len(row):
            return row[idx].strip()
        return ""

    nombres = get("nombres")
    apellidos = get("apellidos")
    cedula = get("cedula")
    tipo_sancion = get("tipo_sancion")
    fecha_sancion = get("fecha_sancion")
    fecha_vencimiento = get("fecha_vencimiento")
    estado = get("estado").upper()

    if not nombres and not cedula:
        return None  # Fila vacía o encabezado repetido

    activo = estado in ("VIGENTE", "ACTIVO", "ACTIVE", "S", "SI", "YES", "1", "TRUE", "")
    if estado in ("INACTIVO", "VENCIDO", "CANCELADO", "N", "NO", "0", "FALSE"):
        activo = False

    return build_canonical(
        fuente=fuente,
        tipo_sujeto="PERSONA_NATURAL",  # PACO registra solo personas naturales
        nombres=nombres or apellidos,
        apellidos=apellidos if nombres else None,
        aliases=[],
        fecha_nacimiento=None,
        nacionalidad=["CO"],  # Procuraduría y Fiscalía de Colombia
        numero_documento=cedula if cedula else None,
        tipo_sancion=tipo_sancion or f"SANCION {fuente}",
        fecha_sancion=fecha_sancion,
        fecha_vencimiento=fecha_vencimiento,
        activo=activo,
        id_fuente_original=f"{fuente}_{row_idx}_{cedula}",
    )


def _parse_csv_bytes(csv_bytes: bytes, fuente: str) -> List[Dict]:
    """Parsea bytes CSV (detectando encoding automáticamente)"""
    # Intentar encodings comunes para archivos colombianos
    for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            text = csv_bytes.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = csv_bytes.decode("latin-1", errors="replace")

    # Detectar delimitador (coma, punto y coma o pipe)
    sample = text[:2000]
    delimiter = ";"
    if sample.count(",") > sample.count(";") and sample.count(",") > sample.count("|"):
        delimiter = ","
    elif sample.count("|") > sample.count(";"):
        delimiter = "|"

    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    rows = list(reader)

    if not rows:
        logger.warning("%s: CSV vacío", fuente)
        return []

    headers = rows[0]
    col_map = _map_columns(headers)

    records = []
    for i, row in enumerate(rows[1:], start=1):
        if not any(cell.strip() for cell in row):
            continue
        try:
            rec = _row_to_record(row, col_map, fuente, i)
            if rec:
                records.append(rec)
        except Exception as e: # pylint: disable=broad-exception-caught
            logger.warning("%s: error en fila %d: %s", fuente, i, e)

    return records


def parse_disc(zip_bytes: bytes) -> List[Dict]:
    """
    - Parsea el ZIP de sanciones disciplinarias de la Procuraduría
    - Extrae el primer archivo CSV o TXT (con formato CSV) del ZIP
    """
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            # Aceptar .csv o .txt
            data_files = [
                n for n in zf.namelist()
                if n.lower().endswith((".csv", ".txt"))
            ]

            if not data_files:
                logger.error("PACO_DISC: No se encontró CSV/TXT dentro del ZIP")
                return []

            logger.info("PACO_DISC: archivos en ZIP: %s", zf.namelist())

            selected_file = data_files[0]
            logger.info("PACO_DISC: archivo seleccionado: %s", selected_file)

            csv_bytes = zf.read(selected_file)

    except zipfile.BadZipFile as e:
        logger.error("PACO_DISC: ZIP inválido: %s", e)
        return []

    records = _parse_csv_bytes(csv_bytes, "PACO_DISC")
    logger.info("PACO_DISC: %d registros normalizados", len(records))
    return records


def parse_penal(csv_bytes: bytes) -> List[Dict]:
    """Parsea el CSV de sanciones penales de la Fiscalía"""
    records = _parse_csv_bytes(csv_bytes, "PACO_PENAL")
    logger.info("PACO_PENAL: %d registros normalizados", len(records))
    return records
