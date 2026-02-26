"""
Capa de persistencia local usando SQLite
    - sanctions: tabla principal con todos los campos del esquema canónico
    - ingestion_log: historial de cargas para detección de cambios
    - match_alerts: alertas generadas por el motor de matching
"""

import sqlite3
import json
import os
from typing import List, Dict, Tuple

from pipeline.utils import get_logger




logger = get_logger("db")


def get_connection(db_path:str) -> sqlite3.Connection:
    """Obtener conexión a la base de datos"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def initialize(db_path:str) -> None:
    """Crea las tablas si no existen"""
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = get_connection(db_path)
    with conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS sanctions (
                id_registro        TEXT PRIMARY KEY,
                fuente             TEXT NOT NULL,
                tipo_sujeto        TEXT NOT NULL,
                nombres            TEXT,
                apellidos          TEXT,
                aliases            TEXT,                    -- JSON array
                fecha_nacimiento   TEXT,
                nacionalidad       TEXT,                    -- JSON array
                numero_documento   TEXT,
                tipo_sancion       TEXT,
                fecha_sancion      TEXT,
                fecha_vencimiento  TEXT,
                activo             INTEGER,
                fecha_ingesta      TEXT,
                hash_contenido     TEXT,
                id_fuente_original TEXT,
                estado_carga       TEXT DEFAULT 'NUEVO'
            );
            CREATE INDEX IF NOT EXISTS idx_sanctions_fuente
                ON sanctions(fuente);
            CREATE INDEX IF NOT EXISTS idx_sanctions_nombres
                ON sanctions(nombres);
            CREATE INDEX IF NOT EXISTS idx_sanctions_documento
                ON sanctions(numero_documento);
            CREATE INDEX IF NOT EXISTS idx_sanctions_hash
                ON sanctions(hash_contenido);

            CREATE TABLE IF NOT EXISTS ingestion_log (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                fuente                  TEXT NOT NULL,
                timestamp_inicio        TEXT NOT NULL,
                timestamp_fin           TEXT,
                registros_total         INTEGER,
                registros_nuevos        INTEGER,
                registros_modificados   INTEGER,
                registros_eliminados    INTEGER,
                errores                 INTEGER,
                duracion_segundos       REAL,
                estado                  TEXT
            );

            CREATE TABLE IF NOT EXISTS match_alerts (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                id_tercero          TEXT NOT NULL,
                id_registro         TEXT NOT NULL,
                fuente              TEXT NOT NULL,
                tipo_match          TEXT NOT NULL,
                score_similitud     REAL,
                nombre_tercero      TEXT,
                nombre_lista        TEXT,
                requiere_revision   INTEGER,
                timestamp           TEXT
            );

            CREATE TABLE IF NOT EXISTS terceros (
                id_tercero          TEXT PRIMARY KEY,
                tipo_sujeto         TEXT,
                nombres             TEXT,
                apellidos           TEXT,
                fecha_nacimiento    TEXT,
                nacionalidad        TEXT,
                numero_documento    TEXT,
                tipo_documento      TEXT,
                pais_residencia     TEXT,
                es_match_plantado   INTEGER DEFAULT 0,
                tipo_match_plantado TEXT
            );
        """)
    conn.close()
    logger.info("Base de datos inicializada en '%s'", db_path)


def upsert_sanctions(conn:sqlite3.Connection, records:List[Dict]) -> Tuple[int, int, int]:
    """
    - Inserta o actualiza registros de sanciones
    - Detecta cambios mediante hash_contenido
    - Retorna (nuevos, modificados, sin_cambios)
    """
    nuevos = modificados = sin_cambios = 0

    for rec in records:
        existing = conn.execute(
            "SELECT hash_contenido FROM sanctions WHERE id_registro = ?",
            (rec["id_registro"],)
        ).fetchone()

        aliases_json = json.dumps(rec.get("aliases", []), ensure_ascii=False)
        nacionalidad_json = json.dumps(rec.get("nacionalidad", []), ensure_ascii=False)

        if existing is None:
            conn.execute("""
                INSERT INTO sanctions (
                    id_registro, fuente, tipo_sujeto, nombres, apellidos,
                    aliases, fecha_nacimiento, nacionalidad, numero_documento,
                    tipo_sancion, fecha_sancion, fecha_vencimiento, activo,
                    fecha_ingesta, hash_contenido, id_fuente_original, estado_carga
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                rec["id_registro"], rec["fuente"], rec["tipo_sujeto"],
                rec.get("nombres"), rec.get("apellidos"),
                aliases_json, rec.get("fecha_nacimiento"),
                nacionalidad_json, rec.get("numero_documento"),
                rec.get("tipo_sancion"), rec.get("fecha_sancion"),
                rec.get("fecha_vencimiento"), int(rec.get("activo", True)),
                rec.get("fecha_ingesta"), rec["hash_contenido"],
                rec.get("id_fuente_original"), "NUEVO"
            ))
            nuevos += 1
        elif existing["hash_contenido"] != rec["hash_contenido"]:
            conn.execute("""
                UPDATE sanctions SET
                    nombres=?, apellidos=?, aliases=?, fecha_nacimiento=?,
                    nacionalidad=?, numero_documento=?, tipo_sancion=?,
                    fecha_sancion=?, fecha_vencimiento=?, activo=?,
                    fecha_ingesta=?, hash_contenido=?, estado_carga=?
                WHERE id_registro=?
            """, (
                rec.get("nombres"), rec.get("apellidos"),
                aliases_json, rec.get("fecha_nacimiento"),
                nacionalidad_json, rec.get("numero_documento"),
                rec.get("tipo_sancion"), rec.get("fecha_sancion"),
                rec.get("fecha_vencimiento"), int(rec.get("activo", True)),
                rec.get("fecha_ingesta"), rec["hash_contenido"],
                "MODIFICADO", rec["id_registro"]
            ))
            modificados += 1
        else:
            sin_cambios += 1

    return nuevos, modificados, sin_cambios


def mark_deleted(conn:sqlite3.Connection, fuente:str, current_ids:set) -> int:
    """
    - Marca como ELIMINADO los registros de una fuente que ya no aparecen en la carga actual
    """
    existing_ids = {
        row[0] for row in conn.execute(
            "SELECT id_registro FROM sanctions WHERE fuente=? AND estado_carga != 'ELIMINADO'",
            (fuente,)
        )
    }
    deleted_ids = existing_ids - current_ids
    if deleted_ids:
        placeholders = ",".join("?" * len(deleted_ids))
        conn.execute(
            f"UPDATE sanctions SET estado_carga='ELIMINADO', activo=0 "
            f"WHERE id_registro IN ({placeholders})",
            list(deleted_ids)
        )
    return len(deleted_ids)


def get_all_sanctions(conn:sqlite3.Connection) -> List[Dict]:
    """Retorna todos los registros activos de sanciones"""
    rows = conn.execute(
        "SELECT * FROM sanctions WHERE estado_carga != 'ELIMINADO'"
    ).fetchall()
    result = []
    for row in rows:
        d = dict(row)
        d["aliases"] = json.loads(d.get("aliases") or "[]")
        d["nacionalidad"] = json.loads(d.get("nacionalidad") or "[]")
        result.append(d)
    return result


def insert_terceros(conn:sqlite3.Connection, records:List[Dict]) -> None:
    """Inserta la base sintética de terceros"""
    conn.executemany("""
        INSERT OR REPLACE INTO terceros (
            id_tercero, tipo_sujeto, nombres, apellidos, fecha_nacimiento,
            nacionalidad, numero_documento, tipo_documento, pais_residencia,
            es_match_plantado, tipo_match_plantado
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, [
        (
            r["id_tercero"], r["tipo_sujeto"], r["nombres"], r.get("apellidos"),
            r.get("fecha_nacimiento"), r.get("nacionalidad"), r.get("numero_documento"),
            r.get("tipo_documento"), r.get("pais_residencia"),
            int(r.get("es_match_plantado", 0)), r.get("tipo_match_plantado")
        )
        for r in records
    ])


def insert_alert(conn:sqlite3.Connection, alert:Dict) -> None:
    """Inserta las alertas"""
    conn.execute("""
        INSERT INTO match_alerts (
            id_tercero, id_registro, fuente, tipo_match, score_similitud,
            nombre_tercero, nombre_lista, requiere_revision, timestamp
        ) VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        alert["id_tercero"], alert["id_registro"], alert["fuente"],
        alert["tipo_match"], alert.get("score_similitud"),
        alert.get("nombre_tercero"), alert.get("nombre_lista"),
        int(alert.get("requiere_revision", False)), alert.get("timestamp")
    ))


def log_ingestion(conn:sqlite3.Connection, log:Dict) -> None:
    """Tabla que sirve como reporte de cada ingestión realizada"""
    conn.execute("""
        INSERT INTO ingestion_log (
            fuente, timestamp_inicio, timestamp_fin, registros_total,
            registros_nuevos, registros_modificados, registros_eliminados,
            errores, duracion_segundos, estado
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        log["fuente"], log["timestamp_inicio"], log.get("timestamp_fin"),
        log.get("registros_total", 0), log.get("registros_nuevos", 0),
        log.get("registros_modificados", 0), log.get("registros_eliminados", 0),
        log.get("errores", 0), log.get("duracion_segundos"), log.get("estado", "OK")
    ))
