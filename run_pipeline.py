"""
Pipeline principal de sanciones
    python run_pipeline.py --env local
    python run_pipeline.py --env local --skip-download      # usa caché
    python run_pipeline.py --env local --only-matching      # solo matching
    python run_pipeline.py --env local --sources OFAC,UN    # fuentes específicas
"""

import argparse
import os
import sys
import time
from datetime import datetime
from typing import List, Dict
import importlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipeline.utils import get_logger # pylint: disable=wrong-import-position
from pipeline.downloader import download_bytes # pylint: disable=wrong-import-position
from pipeline.normalizacion.db import (  # pylint: disable=wrong-import-position
    initialize, get_connection, upsert_sanctions,
    mark_deleted
)
from pipeline.fuentes.fcpa import parse_page # pylint: disable=wrong-import-position
from pipeline.fuentes.paco import parse_disc, parse_penal # pylint: disable=wrong-import-position
from pipeline.fuentes.worldbank import web_scraper # pylint: disable=wrong-import-position





logger = get_logger("pipeline")

CONFIGS = {
    "local": {
        "db_path": "data/sanctions.db",
        "raw_dir": "data/raw",
        "output_dir": "reportes",
        "match_threshold_high": 0.92,
        "match_threshold_low": 0.80,
    }
}

SOURCES_CONFIG = {
    "OFAC": {
        "url": "https://www.treasury.gov/ofac/downloads/sdn.xml",
        "format": "xml",
        "cache_file": "ofac_sdn.xml",
    },
    "UN": {
        "url": "https://scsanctions.un.org/resources/xml/en/consolidated.xml",
        "format": "xml",
        "cache_file": "un_consolidated.xml",
    },
    "EU": {
        "url": "https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content",
        "format": "xml",
        "cache_file": "eu_sanctions.xml",
    },
    "FCPA": {
        "url": '''https://efts.sec.gov/LATEST/search-index?q="fcpa"&forms=AP,10-K,8-K,10-Q''',
        "format": "json_paged",
        "cache_file": "fcpa_page0.json",
        "page_size": 100,
    },
    "PACO_DISC": {
        "url": "https://paco7public7info7prod.blob.core.windows.net/paco-pulic-info/antecedentes_SIRI_sanciones_Cleaned.zip", # pylint: disable=line-too-long
        "format": "zip_csv",
        "cache_file": "paco_disc.zip",
    },
    "PACO_PENAL": {
        "url": "https://paco7public7info7prod.blob.core.windows.net/paco-pulic-info/sanciones_penales_FGN.csv", # pylint: disable=line-too-long
        "format": "csv",
        "cache_file": "paco_penal.csv",
    },
    "WORLD_BANK": {
        "url": "https://projects.worldbank.org/en/projects-operations/procurement/debarred-firms",
        "format": "html_scraped",
        "cache_file": None,  # El scraper maneja su propio estado
    },
}




def _ingest_xml(source_name, config, raw_dir, skip_download, module_path):
    parser_module = importlib.import_module(module_path)
    cache = os.path.join(raw_dir, config["cache_file"])
    if skip_download and os.path.exists(cache):
        with open(cache, "rb") as f:
            raw = f.read()
    else:
        raw = download_bytes(config["url"])
        if raw is None:
            logger.error("%s: descarga fallida", source_name)
            return []
        os.makedirs(raw_dir, exist_ok=True)
        with open(cache, "wb") as f:
            f.write(raw)
    return parser_module.parse(raw)


def _ingest_fcpa(config, raw_dir, skip_download):
    all_records = []
    page_size = config.get("page_size", 1000)
    base_url = config["url"]
    page = 0

    while True:
        cache = os.path.join(raw_dir, f"fcpa_page{page}.json")

        if skip_download and os.path.exists(cache):
            with open(cache, "rb") as f:
                raw = f.read()
        else:
            raw = download_bytes(base_url + f"&from={page * page_size}&hits.hits._source=true")
            if raw is None:
                break
            os.makedirs(raw_dir, exist_ok=True)
            with open(cache, "wb") as f:
                f.write(raw)

        records, total = parse_page(raw)
        all_records.extend(records)
        logger.info("FCPA: página '%s' → %d registros (total: %d)",
                    page, len(records), total)

        if not records or (page + 1) * page_size >= total:
            break
        if page >= 50:  # safety limit
            logger.warning("FCPA: límite de 50 páginas alcanzado")
            break
        page += 1

    return all_records


def _ingest_paco_disc(config, raw_dir, skip_download):
    cache = os.path.join(raw_dir, config["cache_file"])
    if skip_download and os.path.exists(cache):
        with open(cache, "rb") as f:
            raw = f.read()
    else:
        raw = download_bytes(config["url"])
        if raw is None:
            return []
        os.makedirs(raw_dir, exist_ok=True)
        with open(cache, "wb") as f:
            f.write(raw)
    return parse_disc(raw)


def _ingest_paco_penal(config, raw_dir, skip_download):
    cache = os.path.join(raw_dir, config["cache_file"])
    if skip_download and os.path.exists(cache):
        with open(cache, "rb") as f:
            raw = f.read()
    else:
        raw = download_bytes(config["url"])
        if raw is None:
            return []
        os.makedirs(raw_dir, exist_ok=True)
        with open(cache, "wb") as f:
            f.write(raw)
    return parse_penal(raw)


def _ingest_worldbank(config):
    return web_scraper(config["url"])


def ingest_source(source_name: str, config: Dict, raw_dir: str, skip_download: bool) -> List[Dict]:
    """Descarga, parsea y normaliza una fuente. Retorna registros canónicos"""
    logger.info("="*50)
    logger.info("Procesando fuente: %s", source_name)

    try:
        if source_name == "OFAC":
            return _ingest_xml(source_name, config, raw_dir, skip_download, "pipeline.fuentes.ofac")
        elif source_name == "UN":
            return _ingest_xml(source_name, config, raw_dir, skip_download, "pipeline.fuentes.un")
        elif source_name == "EU":
            return _ingest_xml(source_name, config, raw_dir, skip_download, "pipeline.fuentes.eu")
        elif source_name == "FCPA":
            return _ingest_fcpa(config, raw_dir, skip_download)
        elif source_name == "PACO_DISC":
            return _ingest_paco_disc(config, raw_dir, skip_download)
        elif source_name == "PACO_PENAL":
            return _ingest_paco_penal(config, raw_dir, skip_download)
        elif source_name == "WORLD_BANK":
            return _ingest_worldbank(config)
        else:
            logger.error("Fuente desconocida: %s", source_name)
            return []
    except Exception as e: # pylint: disable=broad-exception-caught
        logger.error("Error ingesting %s: %s", source_name, e, exc_info=True)
        return []



















def main():
    """Orquestación del pipeline completo"""
    parser = argparse.ArgumentParser(description="Pipeline de Sanciones y Riesgos")
    parser.add_argument("--env", default="local", choices=list(CONFIGS.keys()))
    parser.add_argument("--skip-download", action="store_true",
                        help="Usar archivos cacheados en lugar de descargar")
    parser.add_argument("--only-matching", action="store_true",
                        help="Solo ejecutar matching (asume DB ya populada)")
    parser.add_argument("--sources", default=None,
                        help="Fuentes a procesar separadas por coma "
                        "(OFAC,UN,EU,FCPA,PACO_DISC,PACO_PENAL,WORLD_BANK)")
    args = parser.parse_args()

    cfg = CONFIGS[args.env]
    run_ts = datetime.now().isoformat()

    logger.info("="*60)
    logger.info("PIPELINE DE SANCIONES — env=%s", args.env)
    logger.info("="*60)

    # Inicializar DB
    initialize(cfg["db_path"])

    # Seleccionar fuentes
    sources_to_run = (
        [s.strip() for s in args.sources.split(",")]
        if args.sources
        else list(SOURCES_CONFIG.keys())
    )

    ingestion_summary = {
        "run_timestamp": run_ts,
        "records_per_source": {},
        "nuevos_per_source": {},
        "modificados_per_source": {},
        "eliminados_per_source": {},
        "errores_per_source": {},
        "duracion_per_source": {},
    }

    all_records_for_quality = []

    # PARTE 1: Ingesta ETL
    if not args.only_matching:
        conn = get_connection(cfg["db_path"])

        for source_name in sources_to_run:
            src_cfg = SOURCES_CONFIG.get(source_name)
            if not src_cfg:
                logger.warning("Fuente no configurada: %s", source_name)
                continue

            t_src = time.time()
            records = ingest_source(
                source_name, src_cfg, cfg["raw_dir"], args.skip_download
            )
            duracion = time.time() - t_src

            if not records:
                logger.warning("%s: 0 registros obtenidos", source_name)
                ingestion_summary["records_per_source"][source_name] = 0
                ingestion_summary["errores_per_source"][source_name] = 1
                continue

            # Detectar cambios y cargar
            with conn:
                nuevos, modificados, _ = upsert_sanctions(conn, records)
                current_ids = {r["id_registro"] for r in records}
                eliminados = mark_deleted(conn, source_name, current_ids)

            ingestion_summary["records_per_source"][source_name] = len(records)
            ingestion_summary["nuevos_per_source"][source_name] = nuevos
            ingestion_summary["modificados_per_source"][source_name] = modificados
            ingestion_summary["eliminados_per_source"][source_name] = eliminados
            ingestion_summary["errores_per_source"][source_name] = 0
            ingestion_summary["duracion_per_source"][source_name] = round(duracion, 2)

            all_records_for_quality.extend(records)

            logger.info(
                "%s: %d registros | %d nuevos | %d modificados | %d eliminados",
                source_name ,len(records), nuevos, modificados, eliminados,
            )

        conn.close()




if __name__ == "__main__":
    main()
