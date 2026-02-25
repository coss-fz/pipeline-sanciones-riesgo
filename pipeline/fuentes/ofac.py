"""
Parser para la lista SDN de OFAC (XML)
"""

from typing import List, Dict
from lxml import etree

from pipeline.utils import build_canonical, normalize_countries, get_logger




logger = get_logger("fuentes.ofac")

# Namespace del XML de OFAC
NS = {"sdn": "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN"}

# OFAC también exporta sin namespace explícito en algunas versiones
NS_ALT = {}





def _text(element, xpath:str, ns:dict) -> str:
    nodes = element.xpath(xpath, namespaces=ns)
    return (nodes[0].text or "").strip() if nodes else ""


def parse(xml_bytes:bytes) -> List[Dict]:
    """
    - Parsea el XML de OFAC SDN y retorna lista de registros en esquema canónico
    - Soporta tanto el XML con namespace como sin él
    """
    try:
        root = etree.fromstring(xml_bytes) # pylint: disable=c-extension-no-member
    except etree.XMLSyntaxError as e: # pylint: disable=c-extension-no-member
        logger.error("Error parseando XML OFAC: %s", e)
        return []

    # Detectar si el XML usa namespace
    tag = root.tag
    if "{" in tag:
        ns_uri = tag.split("}")[0].lstrip("{")
        ns = {"sdn": ns_uri}
        entry_xpath = ".//sdn:sdnEntry"
    else:
        ns = {}
        entry_xpath = ".//sdnEntry"

    def t(el, path):
        """Helper local para extraer texto con el NS correcto"""
        nodes = el.xpath(path, namespaces=ns)
        if not nodes:
            return ""
        node = nodes[0]
        return (node.text or "").strip() if hasattr(node, "text") else str(node).strip()

    entries = root.xpath(entry_xpath, namespaces=ns)
    logger.info("OFAC: %d entradas encontradas en XML", len(entries))

    records = []
    for entry in entries:
        uid = entry.get("uid") or t(entry, "sdn:uid" if ns else "uid")

        sdn_type = t(entry, "sdn:sdnType" if ns else "sdnType")
        tipo_sujeto = "PERSONA_NATURAL" if sdn_type.lower() == "individual" else "PERSONA_JURIDICA"

        first_name = t(entry, "sdn:firstName" if ns else "firstName")
        last_name = t(entry, "sdn:lastName" if ns else "lastName")

        # Aliases
        alias_prefix = "sdn:" if ns else ""
        aka_nodes = entry.xpath(
            f".//{alias_prefix}akaList/{alias_prefix}aka", namespaces=ns
        )
        aliases = []
        for aka in aka_nodes:
            fn = t(aka, f"{alias_prefix}firstName")
            ln = t(aka, f"{alias_prefix}lastName")
            alias = f"{fn} {ln}".strip() if fn or ln else ""
            if alias:
                aliases.append(alias.upper())

        # Fecha de nacimiento (primera disponible)
        dob_nodes = entry.xpath(
            f".//{alias_prefix}dateOfBirthList/{alias_prefix}dateOfBirthItem/"
            f"{alias_prefix}dateOfBirth",
            namespaces=ns,
        )
        fecha_nac = dob_nodes[0].text.strip() if dob_nodes and dob_nodes[0].text else None

        # Nacionalidades
        nat_nodes = entry.xpath(
            f".//{alias_prefix}nationalityList/{alias_prefix}nationality/"
            f"{alias_prefix}country",
            namespaces=ns,
        )
        nationality_texts = [n.text.strip() for n in nat_nodes if n.text]
        nacionalidades = normalize_countries(nationality_texts)

        # Número de documento (preferir pasaporte o National ID)
        id_nodes = entry.xpath(
            f".//{alias_prefix}idList/{alias_prefix}id", namespaces=ns
        )
        numero_doc = None
        for id_node in id_nodes:
            id_type = t(id_node, f"{alias_prefix}idType")
            id_num = t(id_node, f"{alias_prefix}idNumber")
            if id_num:
                numero_doc = id_num
                if "passport" in id_type.lower() or "national" in id_type.lower():
                    break

        # Programas de sanción
        prog_nodes = entry.xpath(
            f".//{alias_prefix}programList/{alias_prefix}program", namespaces=ns
        )
        programas = [p.text.strip() for p in prog_nodes if p.text]
        tipo_sancion = "; ".join(programas) if programas else "SDN"

        record = build_canonical(
            fuente="OFAC",
            tipo_sujeto=tipo_sujeto,
            nombres=first_name or last_name,
            apellidos=last_name if first_name else None,
            aliases=aliases,
            fecha_nacimiento=fecha_nac,
            nacionalidad=nacionalidades,
            numero_documento=numero_doc,
            tipo_sancion=tipo_sancion,
            fecha_sancion=None,  # SDN XML no provee fecha de inclusión
            fecha_vencimiento=None,
            activo=True,
            id_fuente_original=str(uid),
        )
        records.append(record)

    logger.info("OFAC: %d registros normalizados", len(records))
    return records
