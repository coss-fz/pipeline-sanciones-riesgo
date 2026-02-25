"""
Scraper para la lista de firmas inhabilitadas del Banco Mundial
"""

from datetime import datetime
import re
import time
from typing import List, Dict, Tuple
from bs4 import BeautifulSoup

from pipeline.utils import build_canonical, normalize_countries, get_logger, parse_date
from pipeline.downloader import download_bytes




logger = get_logger("fuentes.worldbank")


def _parse_html_page(html:str) -> Tuple[List[Dict], bool]:
    """
    - Parsea una página HTML de la tabla de firmas inhabilitadas
    - Retorna (registros_parciales, hay_mas_paginas)
    """
    soup = BeautifulSoup(html, "lxml")
    records = []

    # Buscar tabla principal
    table = soup.find("table")
    if not table:
        # Intentar divs con clase de tabla
        table = soup.find("div", class_=re.compile(r"table|grid|list", re.I))

    if not table:
        logger.warning("WorldBank: No se encontró tabla HTML")
        return [], False

    rows = table.find_all("tr")
    if not rows:
        return [], False

    # Detectar encabezados
    headers = []
    header_row = rows[0]
    for th in header_row.find_all(["th", "td"]):
        headers.append(th.get_text(strip=True).upper())

    # Mapear columnas
    col_name = next(
        (i for i, h in enumerate(headers) if "FIRM" in h or "NAME" in h or "EMPRESA" in h), 0
    )
    col_country = next(
        (i for i, h in enumerate(headers) if "COUNTRY" in h or "PAIS" in h), None
    )
    col_from = next(
        (i for i, h in enumerate(headers) if "FROM" in h or "DESDE" in h or "START" in h), None
    )
    col_to = next(
        (i for i, h in enumerate(headers) if "TO" in h or "UNTIL" in h or "HASTA" in h or "END" in h), None # pylint: disable=line-too-long
    )
    col_grounds = next(
        (i for i, h in enumerate(headers) if "GROUND" in h or "REASON" in h or "BASIS" in h), None
    )

    for i, row in enumerate(rows[1:], start=1):
        cells = row.find_all(["td", "th"])
        if not cells or len(cells) < 2:
            continue

        def cell(idx):
            if idx is not None and idx < len(cells): # pylint: disable=cell-var-from-loop
                return cells[idx].get_text(strip=True) # pylint: disable=cell-var-from-loop
            return ""

        firm_name = cell(col_name)
        if not firm_name:
            continue

        country = cell(col_country) if col_country is not None else ""
        date_from = cell(col_from) if col_from is not None else ""
        date_to = cell(col_to) if col_to is not None else ""
        grounds = cell(col_grounds) if col_grounds is not None else "DEBARMENT"

        # Determinar activo: si fecha_to está en el futuro o es vacía
        activo = True
        if date_to and date_to.lower() not in ("indefinite", "indefinido", ""):
            try:
                end_date = parse_date(date_to)
                if end_date:
                    activo = end_date >= datetime.now().date().isoformat()
            except Exception: # pylint: disable=broad-exception-caught
                pass

        record = build_canonical(
            fuente="WORLD_BANK",
            tipo_sujeto="PERSONA_JURIDICA",  # World Bank lista principalmente empresas
            nombres=firm_name,
            apellidos=None,
            aliases=[],
            fecha_nacimiento=None,
            nacionalidad=normalize_countries([country]) if country else [],
            numero_documento=None,
            tipo_sancion=grounds or "WORLD BANK DEBARMENT",
            fecha_sancion=date_from,
            fecha_vencimiento=date_to if date_to.lower() not in ("indefinite", "indefinido") else None, # pylint: disable=line-too-long
            activo=activo,
            id_fuente_original=f"WB_{i}_{firm_name[:30]}",
        )
        records.append(record)

    # Detectar si hay más páginas
    has_next = bool(
        soup.find("a", string=re.compile(r"next|siguiente|›|»", re.I))
        or soup.find(class_=re.compile(r"next|pagination", re.I))
    )

    return records, has_next


def web_scraper(url:str, max_pages:int=200) -> List[Dict]:
    """
    Scraping principal de la lista de firmas inhabilitadas del Banco Mundial
    """
    all_records = []
    page_num = 0
    current_url = url

    seen_names = set()

    for page_num in range(1, max_pages + 1):
        logger.info("WorldBank: scraping página %s — %s", page_num, current_url)
        raw = download_bytes(current_url, retries=3, timeout=30)
        if raw is None:
            logger.error("WorldBank: fallo descargando página %s", page_num)
            break

        html = raw.decode("utf-8", errors="replace")
        page_records, has_next = _parse_html_page(html)

        # Deduplicar
        new_records = []
        for r in page_records:
            key = (r.get("nombres", ""), r.get("fecha_sancion", ""))
            if key not in seen_names:
                seen_names.add(key)
                new_records.append(r)

        all_records.extend(new_records)
        logger.info("WorldBank: página %d → %d registros nuevos (total acumulado: %d)",
                    page_num, len(new_records), len(all_records))

        if not has_next or not new_records:
            logger.info("WorldBank: scraping completo — %d páginas, %d registros",
                        page_num,len(all_records))
            break

        # Construir URL de siguiente página
        # El patrón típico es ?page=N o ?start=N*pagesize
        if "page=" in current_url:
            current_url = re.sub(r"page=\d+", f"page={page_num + 1}", current_url)
        else:
            current_url = f"{url}?page={page_num + 1}"

        time.sleep(0.5)  # cortesía al servidor

    logger.info("WorldBank: total final: %d registros en %d páginas", len(all_records), page_num)
    return all_records
