"""
Utilidades compartidas del pipeline de sanciones
"""

import hashlib
import json
import logging
import re
import unicodedata
from datetime import datetime
from typing import Optional, List, Any
import colorlog




# Mapeo de nombres comunes / abreviaturas → ISO 3166-1 alpha-2
_COUNTRY_MAP = {
    "COLOMBIA": "CO", "UNITED STATES": "US", "USA": "US", "U.S.": "US",
    "RUSSIA": "RU", "RUSSIAN FEDERATION": "RU",
    "CHINA": "CN", "PEOPLE'S REPUBLIC OF CHINA": "CN",
    "IRAN": "IR", "ISLAMIC REPUBLIC OF IRAN": "IR",
    "NORTH KOREA": "KP", "DEMOCRATIC PEOPLE'S REPUBLIC OF KOREA": "KP",
    "VENEZUELA": "VE", "CUBA": "CU", "SYRIA": "SY", "MYANMAR": "MM",
    "UKRAINE": "UA", "BELARUS": "BY", "AFGHANISTAN": "AF",
    "IRAQ": "IQ", "LIBYA": "LY", "SUDAN": "SD", "SOMALIA": "SO",
    "YEMEN": "YE", "NICARAGUA": "NI", "HAITI": "HT",
    "UNITED KINGDOM": "GB", "UK": "GB", "GERMANY": "DE",
    "FRANCE": "FR", "SPAIN": "ES", "ITALY": "IT",
    "MEXICO": "MX", "BRAZIL": "BR", "ARGENTINA": "AR",
    "CHILE": "CL", "PERU": "PE", "ECUADOR": "EC",
    "BOLIVIA": "BO", "PARAGUAY": "PY", "URUGUAY": "UY",
    "PANAMA": "PA", "COSTA RICA": "CR", "HONDURAS": "HN",
    "EL SALVADOR": "SV", "GUATEMALA": "GT", "DOMINICAN REPUBLIC": "DO",
    "TURKEY": "TR", "INDIA": "IN", "PAKISTAN": "PK",
    "SAUDI ARABIA": "SA", "UAE": "AE", "UNITED ARAB EMIRATES": "AE",
    "NIGERIA": "NG", "SOUTH AFRICA": "ZA", "KENYA": "KE",
    "ETHIOPIA": "ET", "EGYPT": "EG", "MOROCCO": "MA",
    "INDONESIA": "ID", "MALAYSIA": "MY", "THAILAND": "TH",
    "PHILIPPINES": "PH", "VIETNAM": "VN", "JAPAN": "JP",
    "SOUTH KOREA": "KR", "REPUBLIC OF KOREA": "KR",
    "AUSTRALIA": "AU", "CANADA": "CA", "SWITZERLAND": "CH",
    "NETHERLANDS": "NL", "BELGIUM": "BE", "SWEDEN": "SE",
    "NORWAY": "NO", "DENMARK": "DK", "FINLAND": "FI",
    "AUSTRIA": "AT", "POLAND": "PL", "CZECHIA": "CZ",
    "CZECH REPUBLIC": "CZ", "PORTUGAL": "PT", "GREECE": "GR",
    "ISRAEL": "IL", "JORDAN": "JO", "LEBANON": "LB",
    "UNKNOWN": None, "N/A": None, "": None,
}


def parse_date(value:Any) -> Optional[str]:
    """
    - Intenta parsear una fecha de múltiples formatos y retorna ISO 8601 (YYYY-MM-DD)
    - Retorna None si no puede parsear
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in ("", "none", "null", "n/a", "nd", "sin fecha"):
        return None

    # Formatos a probar, del más específico al más general
    formats = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%d-%m-%Y",
        "%Y%m%d",
        "%d %b %Y",
        "%d %B %Y",
        "%b %d, %Y",
        "%B %d, %Y",
        "%Y-%m",
        "%Y",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt)
            return dt.date().isoformat()
        except ValueError:
            continue

    # Último recurso: extraer 4 dígitos como año
    match = re.search(r"\b(19|20)\d{2}\b", text)
    if match:
        return f"{match.group()}-01-01"
    return None


def compute_hash(record:dict) -> str:
    """
    Computa SHA-256 sobre los campos relevantes del registro (excluyendo
    metadatos de pipeline como fecha_ingesta e id_registro)
    """
    fields = [
        "fuente", "tipo_sujeto", "nombres", "apellidos",
        "fecha_nacimiento", "numero_documento", "tipo_sancion",
        "fecha_sancion",
    ]
    payload = {k: record.get(k) for k in fields}
    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


# Logging
def get_logger(name: str) -> logging.Logger:
    """Configura el logger con colores por nivel."""
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = colorlog.StreamHandler()
        formatter = colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s [%(levelname)s]%(reset)s "
            "%(blue)s%(name)s%(reset)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "white",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            },
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(logging.INFO)
    logger.propagate = False  # evita duplicados si usas root logger

    return logger


# Normalización de texto
def normalize_text(text:Optional[str]) -> Optional[str]:
    """
    Normaliza un texto: elimina tildes, convierte a mayúsculas,
    colapsa espacios y elimina caracteres no alfanuméricos (excepto espacios)
    """
    if text is None:
        return None
    text = str(text).strip()
    # NFD: separa caracteres base de sus diacríticos
    nfd = unicodedata.normalize("NFD", text)
    # Eliminar diacríticos (categoría Mn = Mark, nonspacing)
    ascii_text = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    # Mayúsculas y colapso de espacios
    ascii_text = re.sub(r"\s+", " ", ascii_text).upper().strip()
    return ascii_text if ascii_text else None

def normalize_name_for_matching(text: Optional[str]) -> Optional[str]:
    """
    Versión más agresiva: elimina caracteres no alfabéticos para matching.
    """
    if text is None:
        return None
    base = normalize_text(text) or ""
    return re.sub(r"[^A-Z ]", "", base).strip() or None


# Normalización de países
def normalize_country(value:Any) -> Optional[str]:
    """
    - Intenta devolver el código ISO 3166-1 alpha-2 para un país
    - Si ya es un código de 2 letras válido, lo retorna en mayúsculas
    """
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    # Ya es código ISO 2
    if len(text) == 2 and text.isalpha():
        return text
    # Buscar en mapa (removiendo tildes primero)
    normalized = normalize_text(text)
    if normalized in _COUNTRY_MAP:
        return _COUNTRY_MAP[normalized]
    # Búsqueda parcial por inicio
    for key, code in _COUNTRY_MAP.items():
        if key and normalized and key.startswith(normalized[:4]):
            return code
    return text[:2] if len(text) >= 2 else None

def normalize_countries(value:Any) -> List[str]:
    """
    Convierte una lista o string separado por comas/punto-y-coma a lista de códigos ISO
    """
    if value is None:
        return []
    if isinstance(value, list):
        return [c for c in (normalize_country(v) for v in value) if c]
    text = str(value)
    # Separar por coma, punto y coma o barra
    parts = re.split(r"[,;/]", text)
    return [c for c in (normalize_country(p.strip()) for p in parts if p.strip()) if c]


# Construcción de registro canónico
def build_canonical(
    fuente:str,
    tipo_sujeto:str,
    nombres:Optional[str],
    apellidos:Optional[str],
    aliases:Optional[List[str]],
    fecha_nacimiento:Optional[str],
    nacionalidad:Optional[List[str]],
    numero_documento:Optional[str],
    tipo_sancion:Optional[str],
    fecha_sancion:Optional[str],
    fecha_vencimiento:Optional[str],
    activo:bool,
    id_fuente_original:Optional[str]=None,
) -> dict:
    """
    - Construye un registro normalizado al esquema canónico
    - Genera id_registro como SHA-256 truncado de (fuente + id_fuente_original + nombres)
    """
    now = datetime.utcnow().isoformat()
    record = {
        "fuente": fuente,
        "tipo_sujeto": tipo_sujeto,
        "nombres": normalize_text(nombres),
        "apellidos": normalize_text(apellidos),
        "aliases": aliases or [],
        "fecha_nacimiento": parse_date(fecha_nacimiento),
        "nacionalidad": nacionalidad or [],
        "numero_documento": numero_documento,
        "tipo_sancion": normalize_text(tipo_sancion),
        "fecha_sancion": parse_date(fecha_sancion),
        "fecha_vencimiento": parse_date(fecha_vencimiento),
        "activo": activo,
        "fecha_ingesta": now,
        "id_fuente_original": id_fuente_original,
    }
    record["hash_contenido"] = compute_hash(record)
    # ID único: hash de fuente + id_original + nombre para estabilidad entre cargas
    id_seed = f"{fuente}|{id_fuente_original}|{record['nombres']}"
    record["id_registro"] = hashlib.sha256(id_seed.encode()).hexdigest()[:16]
    return record
