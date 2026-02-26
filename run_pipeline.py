"""
Pipeline principal de sanciones
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from typing import List, Dict
import importlib
import csv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipeline.utils import get_logger # pylint: disable=wrong-import-position
from pipeline.downloader import download_files # pylint: disable=wrong-import-position
from pipeline.normalizacion.db import (  # pylint: disable=wrong-import-position
    initialize, get_connection, upsert_sanctions,
    mark_deleted, get_all_sanctions, insert_terceros,
    insert_alert, log_ingestion
)
from pipeline.calidad.monitoring import MonitoringSystem # pylint: disable=wrong-import-position
from pipeline.calidad.rules import run_quality # pylint: disable=wrong-import-position
from pipeline.matching.engine import MatchingEngine, compute_precision_recall # pylint: disable=wrong-import-position
from pipeline.matching.synthetic import gen_synthetic # pylint: disable=wrong-import-position
from pipeline.fuentes.fcpa import parse_page # pylint: disable=wrong-import-position
from pipeline.fuentes.paco import parse_disc, parse_penal # pylint: disable=wrong-import-position
from pipeline.fuentes.worldbank import web_scraper # pylint: disable=wrong-import-position





logger = get_logger("pipeline")

CONFIGS = {
    "local": {
        "db_path": "data/sanctions.db",
        "raw_dir": "data/raw",
        "output_dir": "reportes",
        "match_threshold_high": 0.92, # 0.92
        "match_threshold_low": 0.60, # 0.80
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
        "url": '''https://efts.sec.gov/LATEST/search-index?q="fcpa"&forms=AP''',
        "format": "json_paged",
        "cache_file": "fcpa_page0.json",
        "page_size": 1000,
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
        raw = download_files(config["url"])
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
            raw = download_files(base_url + f"&from={page * page_size}&hits.hits._source=true")
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
        raw = download_files(config["url"])
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
        raw = download_files(config["url"])
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


def save_report(data:Dict, path:str) -> None:
    """Función para guardar reportes"""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    logger.info("Reporte guardado: '%s'", path)


def save_csv(records: List[Dict], path: str) -> None:
    """Función para guardar archivos en formato CSV"""
    if not records:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    logger.info("CSV guardado: '%s' (%d registros)", path, len(records))




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
    t_pipeline_start = time.time()
    run_ts = datetime.now().isoformat()

    logger.info("="*60)
    logger.info("PIPELINE DE SANCIONES — env=%s", args.env)
    logger.info("="*60)

    # Inicializar DB
    initialize(cfg["db_path"])
    monitor = MonitoringSystem()

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

    # -------------------- PARTE 1: Ingesta ETL --------------------
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

            log_ingestion(conn, {
                "fuente": source_name,
                "timestamp_inicio": run_ts,
                "timestamp_fin": datetime.now().isoformat(),
                "registros_total": len(records),
                "registros_nuevos": nuevos,
                "registros_modificados": modificados,
                "registros_eliminados": eliminados,
                "errores": 0,
                "duracion_segundos": duracion,
                "estado": "OK",
            })

            logger.info(
                "%s: %d registros | %d nuevos | %d modificados | %d eliminados",
                source_name ,len(records), nuevos, modificados, eliminados,
            )

        conn.close()

        ingestion_summary["duration_seconds"] = round(time.time() - t_pipeline_start, 2)

        # Guardar reporte de ingesta
        save_report(ingestion_summary, os.path.join(cfg["output_dir"], "reporte_ingesta.json"))

        # Monitoreo
        monitor.record_ingestion(ingestion_summary)
        monitor.metrics.record("pipeline_duration_seconds", ingestion_summary["duration_seconds"])

        # Calidad de datos
        logger.info("Ejecutando reglas de calidad…")
        quality_report = run_quality(all_records_for_quality)
        save_report(quality_report, os.path.join(cfg["output_dir"], "reporte_calidad.json"))


    # -------------------- PARTE 2: Generación de base sintética y matching --------------------
    logger.info("="*60)
    logger.info("PARTE 2: Base sintética y matching masivo")

    conn = get_connection(cfg["db_path"])
    sanctions = get_all_sanctions(conn)
    logger.info("Sanciones en DB: %d", len(sanctions))

    # Generar base sintética
    synthetic_path = "data/terceros.csv"
    mapa_path = "data/mapa_plantados.json"

    if not os.path.exists(synthetic_path) or not args.skip_download:
        logger.info("Generando base sintética de 10.000 terceros…")
        terceros, mapa_plantados = gen_synthetic(sanctions, seed=42)
        save_csv(terceros, synthetic_path)
        with open(mapa_path, "w", encoding="utf-8") as f:
            json.dump(mapa_plantados, f, ensure_ascii=False, indent=2)
        logger.info("Base sintética generada: %d registros, %d plantados",
                    len(terceros), len(mapa_plantados))
    else:
        logger.info("Cargando base sintética desde '%s'…", synthetic_path)
        terceros = []
        with open(synthetic_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["es_match_plantado"] = int(row.get("es_match_plantado", 0))
                terceros.append(row)
        with open(mapa_path, encoding="utf-8") as f:
            mapa_plantados = json.load(f)

    # Persistir terceros en DB
    with conn:
        insert_terceros(conn, terceros)

    # Matching masivo
    logger.info("Iniciando matching masivo…")
    t_match = time.time()

    engine = MatchingEngine(
        threshold_high=CONFIGS[args.env]["match_threshold_high"],
        threshold_low=CONFIGS[args.env]["match_threshold_low"],
    )
    engine.load_sanctions(sanctions)
    alerts, match_metrics = engine.run_batch(terceros)

    match_duration = time.time() - t_match
    match_metrics["tiempo_segundos"] = round(match_duration, 2)

    # Persistir alertas
    with conn:
        for alert in alerts:
            insert_alert(conn, alert)

    # Métricas de precisión/recall sobre plantados
    planted = [t for t in terceros if int(t.get("es_match_plantado", 0)) == 1]
    pr_metrics = compute_precision_recall(alerts, planted)
    logger.info("Precisión: %.4f | Recall: %.4f | F1: %.4f | TP=%s FP=%s FN=%s",
        pr_metrics["precision"], pr_metrics["recall"], pr_metrics["f1"],
        pr_metrics["tp"], pr_metrics["fp"], pr_metrics["fn"])

    # Reporte de alertas
    alert_report = {
        "run_timestamp": run_ts,
        "metricas": match_metrics,
        "precision_recall": pr_metrics,
        "total_alertas": len(alerts),
        "alertas_por_tipo": {},
        "alertas_por_fuente": {},
        "alertas_sample": alerts[:10],
    }
    for a in alerts:
        t = a["tipo_match"]
        alert_report["alertas_por_tipo"][t] = alert_report["alertas_por_tipo"].get(t, 0) + 1
        f = a["fuente"]
        alert_report["alertas_por_fuente"][f] = alert_report["alertas_por_fuente"].get(f, 0) + 1

    save_report(alert_report, os.path.join(cfg["output_dir"], "reporte_alertas.json"))
    save_csv(alerts, os.path.join(cfg["output_dir"], "alertas.csv"))

    # Monitoreo final
    monitor.record_matching(match_metrics)
    fired_alerts = monitor.evaluate_alerts()
    monitoring_report = monitor.generate_report()
    save_report(monitoring_report, os.path.join(cfg["output_dir"], "reporte_monitoreo.json"))

    conn.close()


    # -------------------- Resumen final --------------------
    total_duration = time.time() - t_pipeline_start
    logger.info("="*60)
    logger.info("PIPELINE COMPLETADO en %.1fs", total_duration)
    logger.info("Fuentes procesadas: %d", len(ingestion_summary.get("records_per_source", {})))
    logger.info("Total sanciones en DB: %d", len(sanctions))
    logger.info("Terceros sintéticos: %d", len(terceros))
    logger.info("Alertas generadas: %d", len(alerts))
    logger.info("Precision=%.3f Recall=%.3f F1=%.3f",
        pr_metrics["precision"], pr_metrics["recall"], pr_metrics["f1"])
    logger.info("Alertas operacionales disparadas: %d", len(fired_alerts))
    logger.info("="*60)


if __name__ == "__main__":
    main()
