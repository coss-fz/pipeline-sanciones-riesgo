"""
Generador de base sintética de 10.000 terceros
- ~92% registros limpios (personas y empresas ficticias)
- ~8% registros con match deliberado distribuidos entre:
  * Match exacto de nombre
  * Match con variación tipográfica (un carácter diferente)
  * Match con nombre parcial (primer nombre + primer apellido)
  * Match con alias conocido
  * Match por número de documento
"""

import random
from typing import List, Dict, Optional, Tuple

from pipeline.utils import get_logger




logger = get_logger("matching.synthetic")

NOMBRES_MASC = [
    "CARLOS", "JUAN", "MIGUEL", "JOSE", "LUIS", "ANDRES", "PEDRO", "JORGE",
    "DAVID", "DANIEL", "ALEJANDRO", "FERNANDO", "RICARDO", "MARIO", "ROBERTO",
    "HECTOR", "PABLO", "SERGIO", "EDGAR", "OSCAR", "MANUEL", "RAFAEL", "IVAN",
    "GABRIEL", "SEBASTIAN", "CRISTIAN", "FABIAN", "CAMILO", "NICOLAS", "DIEGO",
    "WILLIAM", "HENRY", "JAMES", "JOHN", "ROBERT", "MICHAEL", "CHARLES",
    "ALEXANDER", "VICTOR", "ANTONIO", "FRANCISCO", "ENRIQUE", "ERNESTO",
]

NOMBRES_FEM = [
    "MARIA", "ANA", "LAURA", "CAROLINA", "DIANA", "PATRICIA", "SANDRA", "GLORIA",
    "ANDREA", "PAOLA", "NATALIA", "CLAUDIA", "ALEJANDRA", "MONICA", "LUCIA",
    "ISABELLA", "VALENTINA", "CAMILA", "SOFIA", "DANIELA", "CATALINA", "JESSICA",
    "JENNIFER", "ELIZABETH", "MARGARET", "SARA", "ELENA", "ROSA", "CARMEN",
    "BEATRIZ", "MARTHA", "LILIANA", "ADRIANA", "XIOMARA", "YOLANDA",
]

APELLIDOS = [
    "GARCIA", "RODRIGUEZ", "MARTINEZ", "HERNANDEZ", "LOPEZ", "GONZALEZ",
    "PEREZ", "SANCHEZ", "RAMIREZ", "TORRES", "FLORES", "RIVERA", "GOMEZ",
    "DIAZ", "REYES", "MORALES", "JIMENEZ", "GUTIERREZ", "ORTIZ", "VARGAS",
    "CASTILLO", "RAMOS", "MORENO", "ROMERO", "HERRERA", "MEDINA", "AGUILAR",
    "SUAREZ", "RUIZ", "ROJAS", "VELASQUEZ", "CONTRERAS", "CRUZ", "PATEL",
    "SMITH", "JOHNSON", "WILLIAMS", "BROWN", "JONES", "MILLER", "WILSON",
    "MOORE", "TAYLOR", "ANDERSON", "THOMAS", "JACKSON", "WHITE", "HARRIS",
    "MULLER", "SCHMIDT", "WEBER", "KIM", "CHEN", "WANG", "ZHANG", "LI",
    "SILVA", "FERREIRA", "SOUZA", "LIMA", "COSTA", "ALVES", "BARBOSA",
    "CARDENAS", "NIETO", "PINEDA", "ACOSTA", "MONTOYA", "OSPINA", "CASTAÑO",
    "BUSTOS", "SALAZAR", "BENITEZ", "LEON", "RIOS", "MORA", "GUERRERO",
]

EMPRESAS_BASE = [
    "SOLUCIONES", "SERVICIOS", "INVERSIONES", "CONSULTORES", "COMERCIAL",
    "INDUSTRIAL", "CONSTRUCTORA", "IMPORTADORA", "EXPORTADORA", "DISTRIBUIDORA",
    "TECNOLOGIA", "SISTEMAS", "INNOVACION", "GRUPO", "HOLDING", "VENTURES",
    "CAPITAL", "PARTNERS", "ASOCIADOS", "INGENIERIA", "ARQUITECTURA",
    "LOGISTICA", "TRANSPORTE", "ENERGIA", "RECURSOS", "GLOBAL", "INTER",
    "NACIONAL", "CONTINENTAL", "ANDINA", "CARIBE", "PACIFICO", "ATLANTICO",
]

SUFIJOS_EMPRESA = [
    "S.A.S.", "LTDA.", "S.A.", "INC.", "LLC", "S.A. DE C.V.",
    "CIA LTDA", "S.R.L.", "S.C.", "E.U.",
]

PAISES = ["CO", "US", "MX", "VE", "EC", "PE", "BO", "AR", "CL", "BR", "PA", "CR"]
TIPOS_DOC = ["CC", "CE", "PASAPORTE", "NIT"]

TIPOS_SANCION_PLANTADAS = [
    "EXACTO_NOMBRE", "TIPOGRAFICO", "NOMBRE_PARCIAL", "ALIAS", "EXACTO_DOCUMENTO"
]


def _random_doc(tipo:str, rng:random.Random) -> str:
    if tipo == "CC":
        return str(rng.randint(10_000_000, 99_999_999))
    elif tipo == "CE":
        return str(rng.randint(100_000, 999_999))
    elif tipo == "NIT":
        base = str(rng.randint(800_000_000, 999_999_999))
        return f"{base}-{rng.randint(1,9)}"
    else:  # PASAPORTE
        letters = "ABCDEFGHJKLMNPQRSTUVWXYZ"
        return rng.choice(letters) + str(rng.randint(10_000_000, 99_999_999))


def _gen_persona(id_num:int, rng:random.Random) -> Dict:
    genero = rng.choice(["M", "F"])
    nombre_pool = NOMBRES_MASC if genero == "M" else NOMBRES_FEM
    nombre = rng.choice(nombre_pool)
    ap1 = rng.choice(APELLIDOS)
    ap2 = rng.choice(APELLIDOS)
    tipo_doc = rng.choices(TIPOS_DOC[:3], weights=[60, 20, 20])[0]  # CC más frecuente
    pais = rng.choices(PAISES, weights=[50] + [5]*11)[0]
    year = rng.randint(1950, 2000)
    month = rng.randint(1, 12)
    day = rng.randint(1, 28)

    return {
        "id_tercero": f"T{id_num:06d}",
        "tipo_sujeto": "PERSONA_NATURAL",
        "nombres": nombre,
        "apellidos": f"{ap1} {ap2}",
        "fecha_nacimiento": f"{year}-{month:02d}-{day:02d}",
        "nacionalidad": pais,
        "numero_documento": _random_doc(tipo_doc, rng),
        "tipo_documento": tipo_doc,
        "pais_residencia": pais,
        "es_match_plantado": 0,
        "tipo_match_plantado": None,
    }


def _gen_empresa(id_num:int, rng:random.Random) -> Dict:
    base1 = rng.choice(EMPRESAS_BASE)
    base2 = rng.choice(EMPRESAS_BASE)
    sufijo = rng.choice(SUFIJOS_EMPRESA)
    nombre = f"{base1} {base2} {sufijo}".strip()
    pais = rng.choice(PAISES)
    tipo_doc = "NIT"

    return {
        "id_tercero": f"T{id_num:06d}",
        "tipo_sujeto": "PERSONA_JURIDICA",
        "nombres": nombre,
        "apellidos": None,
        "fecha_nacimiento": None,
        "nacionalidad": pais,
        "numero_documento": _random_doc(tipo_doc, rng),
        "tipo_documento": tipo_doc,
        "pais_residencia": pais,
        "es_match_plantado": 0,
        "tipo_match_plantado": None,
    }


def _typo(name:str, rng:random.Random) -> str:
    """Introduce un carácter diferente o transposición en el nombre"""
    words = name.split()
    if not words:
        return name
    # Elegir palabra a modificar
    w = list(rng.choice(words))
    if len(w) < 3:
        return name
    op = rng.choice(["swap", "substitute", "delete"])
    if op == "swap" and len(w) >= 2:
        i = rng.randint(0, len(w) - 2)
        w[i], w[i+1] = w[i+1], w[i]
    elif op == "substitute":
        i = rng.randint(1, len(w) - 1)
        chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        w[i] = rng.choice([c for c in chars if c != w[i]])
    elif op == "delete" and len(w) > 3:
        i = rng.randint(1, len(w) - 2)
        del w[i]
    modified = "".join(w)
    # Reemplazar en words
    result_words = list(words)
    idx = words.index(rng.choice(words))
    result_words[idx] = modified
    return " ".join(result_words)


def _partial_name(nombres:str, apellidos:Optional[str]) -> str:
    """Retorna solo el primer nombre + primer apellido"""
    n = (nombres or "").split()[0] if nombres else ""
    a = (apellidos or "").split()[0] if apellidos else ""
    return f"{n} {a}".strip()


def generate_planted(
    sanctions:List[Dict],
    n_planted:int,
    id_offset:int,
    rng:random.Random,
) -> Tuple[List[Dict], List[Dict]]:
    """
    - Genera registros plantados derivados de sanciones reales
    - Retorna (terceros_plantados, mapa_de_plantados)
    """
    if not sanctions:
        logger.warning("No hay sanciones reales para generar plantados — usando fallback sintético")
        return _gen_planted_fallback(n_planted, id_offset, rng)

    # Filtrar sanciones con nombre válido
    valid = [s for s in sanctions if s.get("nombres") and len(s.get("nombres","")) > 2]
    if len(valid) < n_planted:
        valid = valid * (n_planted // len(valid) + 2)

    rng.shuffle(valid)
    chunk = n_planted // 5
    planted_terceros = []
    planted_map = []

    tipo_dist = (
        [("EXACTO_NOMBRE", 0)] * chunk +
        [("TIPOGRAFICO", 1)] * chunk +
        [("NOMBRE_PARCIAL", 2)] * chunk +
        [("ALIAS", 3)] * chunk +
        [("EXACTO_DOCUMENTO", 4)] * (n_planted - 4 * chunk)
    )

    for i, (tipo_sancion, _) in enumerate(tipo_dist[:n_planted]):
        src = valid[i % len(valid)]
        id_num = id_offset + i
        nombres_src = src.get("nombres") or ""
        apellidos_src = src.get("apellidos") or ""
        aliases_src = src.get("aliases") or []

        pais = src.get("nacionalidad") or []
        pais = pais[0] if pais else rng.choice(PAISES)

        base = {
            "id_tercero": f"T{id_num:06d}",
            "tipo_sujeto": src.get("tipo_sujeto", "PERSONA_NATURAL"),
            "fecha_nacimiento": src.get("fecha_nacimiento"),
            "nacionalidad": pais,
            "numero_documento": None,
            "tipo_documento": "PASAPORTE",
            "pais_residencia": pais,
            "es_match_plantado": 1,
            "tipo_match_plantado": tipo_sancion,
        }

        if tipo_sancion == "EXACTO_NOMBRE":
            base["nombres"] = nombres_src
            base["apellidos"] = apellidos_src
        elif tipo_sancion == "TIPOGRAFICO":
            full = f"{nombres_src} {apellidos_src}".strip()
            modified = _typo(full, rng)
            parts = modified.split()
            base["nombres"] = parts[0] if parts else nombres_src
            base["apellidos"] = " ".join(parts[1:]) if len(parts) > 1 else apellidos_src
        elif tipo_sancion == "NOMBRE_PARCIAL":
            partial = _partial_name(nombres_src, apellidos_src)
            parts = partial.split()
            base["nombres"] = parts[0] if parts else nombres_src
            base["apellidos"] = parts[1] if len(parts) > 1 else ""
        elif tipo_sancion == "ALIAS":
            if aliases_src:
                alias = rng.choice(aliases_src)
                parts = alias.split()
                base["nombres"] = parts[0] if parts else nombres_src
                base["apellidos"] = " ".join(parts[1:]) if len(parts) > 1 else apellidos_src
            else:
                # Fallback: usar nombre exacto si no hay alias
                base["nombres"] = nombres_src
                base["apellidos"] = apellidos_src
                base["tipo_match_plantado"] = "EXACTO_NOMBRE"
        elif tipo_sancion == "EXACTO_DOCUMENTO":
            doc = src.get("numero_documento")
            if doc:
                base["nombres"] = nombres_src
                base["apellidos"] = apellidos_src
                base["numero_documento"] = doc
                base["tipo_documento"] = "PASAPORTE"
            else:
                # Si no hay documento en la fuente, fallback a exacto nombre
                base["nombres"] = nombres_src
                base["apellidos"] = apellidos_src
                base["tipo_match_plantado"] = "EXACTO_NOMBRE"

        planted_terceros.append(base)
        planted_map.append({
            "id_tercero": base["id_tercero"],
            "id_registro_sancion": src["id_registro"],
            "fuente_sancion": src["fuente"],
            "tipo_match": base["tipo_match_plantado"],
            "nombre_original": f"{nombres_src} {apellidos_src}".strip(),
            "nombre_plantado": f"{base.get('nombres','')} {base.get('apellidos','')}".strip(),
        })

    return planted_terceros, planted_map


def _gen_planted_fallback(n:int, id_offset:int, rng:random.Random) -> Tuple[List, List]:
    """Genera plantados sintéticos cuando no hay sanciones reales"""
    records = []
    mapa = []
    for i in range(n):
        nombre = rng.choice(NOMBRES_MASC + NOMBRES_FEM)
        apellido = rng.choice(APELLIDOS)
        rec = {
            "id_tercero": f"T{id_offset+i:06d}",
            "tipo_sujeto": "PERSONA_NATURAL",
            "nombres": nombre,
            "apellidos": apellido,
            "fecha_nacimiento": None,
            "nacionalidad": "CO",
            "numero_documento": str(rng.randint(10_000_000, 99_999_999)),
            "tipo_documento": "CC",
            "pais_residencia": "CO",
            "es_match_plantado": 1,
            "tipo_match_plantado": "EXACTO_NOMBRE",
        }
        records.append(rec)
        mapa.append({
            "id_tercero": rec["id_tercero"],
            "id_registro_sancion": "FALLBACK",
            "fuente_sancion": "SYNTHETIC",
            "tipo_match": "EXACTO_NOMBRE",
            "nombre_original": f"{nombre} {apellido}",
            "nombre_plantado": f"{nombre} {apellido}",
        })
    return records, mapa


def gen_synthetic(sanctions:List[Dict], seed:int=42) -> Tuple[List[Dict], List[Dict]]:
    """
    - Genera el dataset completo de 10.000 terceros
    - Retorna (terceros, mapa_plantados)
    """
    rng = random.Random(seed)
    total = 10_000
    n_planted = 800
    n_clean = total - n_planted

    logger.info("Generando terceros limpios…")
    clean = []
    for i in range(n_clean):
        if rng.random() < 0.7:  # 70% personas naturales
            clean.append(_gen_persona(i, rng))
        else:
            clean.append(_gen_empresa(i, rng))

    logger.info("Generando registros plantados…")
    planted, mapa = generate_planted(sanctions, n_planted, id_offset=n_clean, rng=rng)

    all_records = clean + planted
    rng.shuffle(all_records)  # mezclar para no dar pistas por posición

    logger.info("Dataset sintético generado: %s registros (%s plantados, %s limpios)",
                len(all_records), n_planted, n_clean)
    return all_records, mapa
