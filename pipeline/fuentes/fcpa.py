"""
Parser para FCPA Enforcement Actions de la SEC (JSON paginado)
"""

import json
import re
from typing import Dict, Optional

from pipeline.utils import build_canonical, get_logger




logger = get_logger("fuentes.fcpa")

# Indicadores de persona jurídica en el nombre
_COMPANY_PATTERNS = re.compile(
    r"\b(LLC|INC|CORP|CORPORATION|LIMITED|LTD|S\.A\.|S\.A|GMBH|B\.V\.|PLC|"
    r"COMPANY|CO\.|GROUP|HOLDINGS|PARTNERS|ASSOCIATES|ENTERPRISES|BANK|"
    r"FUND|TRUST|FOUNDATION|INSTITUTE|SERVICES|SOLUTIONS|TECHNOLOGIES|"
    r"INTERNATIONAL|GLOBAL|WORLDWIDE)\b",
    re.IGNORECASE,
)

_PERSON_PATTERNS = re.compile(r"\b(JR\.|SR\.|III|II|MR\.|MRS\.|DR\.)\b", re.IGNORECASE)


def _infer_tipo_sujeto(name:str) -> str:
    if _COMPANY_PATTERNS.search(name):
        return "PERSONA_JURIDICA"
    if _PERSON_PATTERNS.search(name):
        return "PERSONA_NATURAL"
    # Heurística: si tiene más de 3 palabras sin apellido típico, probablemente empresa
    words = name.strip().split()
    if len(words) <= 3:
        return "PERSONA_NATURAL"
    return "PERSONA_JURIDICA"


def parse_page(json_bytes:bytes) -> tuple:
    """
    - Parsea una página de resultados JSON de la SEC FCPA
    - Retorna (registros, total_hits)
    """
    try:
        data = json.loads(json_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.error("Error parseando JSON FCPA: %s", e)
        return [], 0

    hits_wrapper = data.get("hits", {})
    total = hits_wrapper.get("total", {})
    if isinstance(total, dict):
        total_count = total.get("value", 0)
    else:
        total_count = int(total) if total else 0

    hits = hits_wrapper.get("hits", [])
    records = []

    for hit in hits:
        source = hit.get("_source", {})
        try:
            record = _parse_hit(hit.get("_id", ""), source)
            if record:
                records.append(record)
        except Exception as e: #pylint: disable=broad-exception-caught
            logger.warning("Error procesando hit FCPA %s: %s", hit.get('_id'), e)

    return records, total_count


def _parse_hit(hit_id:str, source:dict) -> Optional[Dict]:
    entity_name = (
        source.get("entity_name")
        or source.get("display_names")
        or source.get("title")
        or source.get("name")
    )
    if not entity_name:
        return None
    if isinstance(entity_name, list):
        entity_name = entity_name[0] if entity_name else None

    file_num = source.get("file_num") or source.get("release_no") or hit_id

    # Fecha
    fecha_sancion = (
        source.get("display_date_filed")
        or source.get("period_of_report")
        or source.get("date_filed")
    )

    # Tipo de acción / sanción
    action_type = source.get("form_type") or source.get("type") or "FCPA"
    tipo_sancion = f"FCPA {action_type}".strip()

    # Tipo de sujeto
    tipo_sujeto = _infer_tipo_sujeto(entity_name)

    # Para personas naturales, intentar separar nombre/apellido
    words = entity_name.strip().split()
    if tipo_sujeto == "PERSONA_NATURAL":
        nombres = words[0] if words else entity_name
        apellidos = " ".join(words[1:]) if len(words) > 1 else None
    else:
        nombres = entity_name
        apellidos = None

    return build_canonical(
        fuente="FCPA",
        tipo_sujeto=tipo_sujeto,
        nombres=nombres,
        apellidos=apellidos,
        aliases=[],
        fecha_nacimiento=None,
        nacionalidad=[],
        numero_documento=None,
        tipo_sancion=tipo_sancion,
        fecha_sancion=fecha_sancion,
        fecha_vencimiento=None,
        activo=True,
        id_fuente_original=str(file_num),
    )
