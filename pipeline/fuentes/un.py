"""
Parser para la lista consolidada de sanciones de la ONU (XML)
"""

from typing import List, Dict
from lxml import etree

from pipeline.utils import build_canonical, normalize_countries, get_logger



logger = get_logger("fuentes.un")


def _get_text(element, *tags) -> str:
    for tag in tags:
        nodes = element.findall(f".//{tag}")
        for node in nodes:
            if node.text and node.text.strip():
                return node.text.strip()
    return ""


def _parse_individual(el) -> Dict:
    uid = _get_text(el, "DATAID") or el.get("DATAID", "")

    parts = []
    for tag in ["FIRST_NAME", "SECOND_NAME", "THIRD_NAME", "FOURTH_NAME"]:
        v = _get_text(el, tag)
        if v:
            parts.append(v)

    full_name = " ".join(parts).strip()
    # Heurística: primera palabra = nombres, resto = apellidos
    words = full_name.split()
    nombres = words[0] if words else full_name
    apellidos = " ".join(words[1:]) if len(words) > 1 else None

    # Aliases
    aliases = []
    for aka in el.findall(".//AKA"):
        alias_parts = []
        for tag in ["FIRST_NAME", "SECOND_NAME", "THIRD_NAME", "FOURTH_NAME"]:
            v = _get_text(aka, tag)
            if v:
                alias_parts.append(v)
        if alias_parts:
            aliases.append(" ".join(alias_parts).upper())

    # Fechas
    fecha_sancion = _get_text(el, "LISTED_ON")

    # VALUE puede pertenecer a muchos campos; filtramos por contexto
    nat_nodes = el.findall(".//NATIONALITY")
    nationality_texts = []
    for nat in nat_nodes:
        for child in nat:
            if child.text and child.text.strip():
                nationality_texts.append(child.text.strip())
    nacionalidades = normalize_countries(nationality_texts)

    # Documentos (pasaporte primero)
    numero_doc = _get_text(el, "PASSPORT_NUMBER", "INDIVIDUAL_DOCUMENT")
    if not numero_doc:
        for doc in el.findall(".//INDIVIDUAL_DOCUMENT"):
            num = _get_text(doc, "NUMBER")
            if num:
                numero_doc = num
                break

    # Tipo sanción
    tipo_sancion = _get_text(el, "JUSTIFICATION") or "UN SANCTIONS"

    # DOB
    fecha_nac = _get_text(el, "DATE_OF_BIRTH")

    return build_canonical(
        fuente="UN",
        tipo_sujeto="PERSONA_NATURAL",
        nombres=nombres,
        apellidos=apellidos,
        aliases=aliases,
        fecha_nacimiento=fecha_nac,
        nacionalidad=nacionalidades,
        numero_documento=numero_doc,
        tipo_sancion=tipo_sancion,
        fecha_sancion=fecha_sancion,
        fecha_vencimiento=None,
        activo=True,
        id_fuente_original=str(uid),
    )


def _parse_entity(el) -> Dict:
    uid = _get_text(el, "DATAID") or el.get("DATAID", "")
    name = _get_text(el, "FIRST_NAME", "ENTITY_NAME")

    aliases = []
    for aka in el.findall(".//AKA"):
        v = _get_text(aka, "FIRST_NAME")
        if v:
            aliases.append(v.upper())

    fecha_sancion = _get_text(el, "LISTED_ON")
    tipo_sancion = _get_text(el, "JUSTIFICATION") or "UN ENTITY SANCTIONS"

    nat_nodes = el.findall(".//ENTITY_ADDRESS")
    nationality_texts = [_get_text(n, "COUNTRY") for n in nat_nodes]
    nationality_texts = [t for t in nationality_texts if t]
    nacionalidades = normalize_countries(nationality_texts)

    return build_canonical(
        fuente="UN",
        tipo_sujeto="PERSONA_JURIDICA",
        nombres=name,
        apellidos=None,
        aliases=aliases,
        fecha_nacimiento=None,
        nacionalidad=nacionalidades,
        numero_documento=None,
        tipo_sancion=tipo_sancion,
        fecha_sancion=fecha_sancion,
        fecha_vencimiento=None,
        activo=True,
        id_fuente_original=str(uid),
    )


def parse(xml_bytes:bytes) -> List[Dict]:
    """
    Parsea el XML consolidado de sanciones ONU
    """
    try:
        root = etree.fromstring(xml_bytes) # pylint: disable=c-extension-no-member
    except etree.XMLSyntaxError as e: # pylint: disable=c-extension-no-member
        logger.error("Error parseando XML ONU: %s", e)
        return []

    records = []

    individuals = root.findall(".//INDIVIDUAL")
    logger.info("UN: %d individuos encontrados", len(individuals))
    for el in individuals:
        try:
            records.append(_parse_individual(el))
        except Exception as e: # pylint: disable=broad-exception-caught
            logger.warning("Error procesando individuo UN: %s", e)

    entities = root.findall(".//ENTITY")
    logger.info("UN: %d entidades encontradas", len(entities))
    for el in entities:
        try:
            records.append(_parse_entity(el))
        except Exception as e: # pylint: disable=broad-exception-caught
            logger.warning("Error procesando entidad UN: %s", e)

    logger.info("UN: %d registros normalizados", len(records))
    return records
