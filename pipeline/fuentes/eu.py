"""
Parser para la lista de sanciones financieras de la UE (XML)
"""

from typing import List, Dict, Optional
from lxml import etree

from pipeline.utils import build_canonical, normalize_countries, get_logger




logger = get_logger("fuentes.eu")


def _attr(el, *attrs) -> str:
    for a in attrs:
        v = el.get(a)
        if v:
            return v.strip()
    return ""


def _child_text(el, tag) -> str:
    child = el.find(tag)
    return (child.text or "").strip() if child is not None and child.text else ""


def _parse_entity(ent) -> Optional[Dict]:
    logical_id = _attr(ent, "logicalId", "euReferenceNumber", "id")

    # Tipo de sujeto
    subject_type_el = ent.find(".//subjectType")
    if subject_type_el is None:
        subject_type_el = ent.find("subjectType")
    if subject_type_el is not None:
        code = _attr(subject_type_el, "code", "classificationCode").lower()
        tipo_sujeto = "PERSONA_NATURAL" if "person" in code else "PERSONA_JURIDICA"
    else:
        tipo_sujeto = "PERSONA_JURIDICA"

    # Nombres y aliases
    name_aliases = ent.findall(".//nameAlias") or ent.findall("nameAlias")
    primary_name = None
    aliases = []

    for na in name_aliases:
        whole = _attr(na, "wholeName", "lastName", "firstName")
        if not whole:
            fn = _attr(na, "firstName")
            ln = _attr(na, "lastName")
            whole = f"{fn} {ln}".strip()
        if not whole:
            continue
        strong = _attr(na, "strong").lower()
        if strong == "true" and primary_name is None:
            primary_name = whole
        else:
            aliases.append(whole.upper())

    if primary_name is None and aliases:
        primary_name = aliases.pop(0)
    if primary_name is None:
        return None

    # Separar nombres / apellidos para personas naturales
    words = primary_name.strip().split()
    if tipo_sujeto == "PERSONA_NATURAL":
        nombres = words[0] if words else primary_name
        apellidos = " ".join(words[1:]) if len(words) > 1 else None
    else:
        nombres = primary_name
        apellidos = None

    # Fecha nacimiento
    fecha_nac = None
    dob_el = ent.find(".//birthdate") or ent.find("birthdate")
    if dob_el is not None:
        fecha_nac = _attr(dob_el, "birthdate", "date", "year")

    # Ciudadanías / nacionalidades
    cit_els = ent.findall(".//citizenship") or ent.findall("citizenship")
    nat_texts = [_attr(c, "countryIso2Code", "countryDescription") for c in cit_els]
    # Addresses como fallback
    addr_els = ent.findall(".//address") or ent.findall("address")
    nat_texts += [_attr(a, "countryIso2Code", "countryDescription") for a in addr_els]
    nat_texts = [t for t in nat_texts if t]
    nacionalidades = normalize_countries(nat_texts)

    # Documentos
    id_els = ent.findall(".//identification") or ent.findall("identification")
    numero_doc = None
    for id_el in id_els:
        num = _attr(id_el, "number")
        if num:
            numero_doc = num
            id_type = _attr(id_el, "identificationTypeCode", "identificationTypeDescription").lower()
            if "passport" in id_type or "national" in id_type:
                break

    # Sanción
    reg_el = ent.find(".//regulation") or ent.find("regulation")
    fecha_sancion = None
    tipo_sancion = "EU FINANCIAL SANCTIONS"
    if reg_el is not None:
        fecha_sancion = _attr(reg_el, "entryIntoForceDate", "publicationDate")
        prog = _attr(reg_el, "programme")
        reg_type_el = reg_el.find("regulationType")
        if reg_type_el is not None:
            reg_code = _attr(reg_type_el, "code", "description")
            tipo_sancion = f"{prog} {reg_code}".strip() or tipo_sancion

    return build_canonical(
        fuente="EU",
        tipo_sujeto=tipo_sujeto,
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
        id_fuente_original=str(logical_id),
    )


def parse(xml_bytes:bytes) -> List[Dict]:
    """Parsea el XML de sanciones financieras de la UE"""
    try:
        root = etree.fromstring(xml_bytes) # pylint: disable=c-extension-no-member
    except etree.XMLSyntaxError as e: # pylint: disable=c-extension-no-member
        logger.error("Error parseando XML EU: %s", e)
        return []

    # El root puede ser <export> o <sanctionsList>
    # Buscar todas las entidades
    entities = (
        root.findall(".//sanctionEntity")
        or root.findall(".//entity")
        or root.findall(".//Entity")
    )
    logger.info("EU: %d entidades encontradas", len(entities))

    records = []
    for ent in entities:
        try:
            record = _parse_entity(ent)
            if record:
                records.append(record)
        except Exception as e: #pylint: disable=broad-exception-caught
            logger.warning("Error procesando entidad EU: %s", e)

    logger.info("EU: %d registros normalizados", len(records))
    return records
