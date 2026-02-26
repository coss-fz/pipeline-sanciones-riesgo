"""
Sistema de monitoreo operacional (implementación mock/stub)

Métricas rastreadas:
    1. Registros por fuente (conteo total y delta respecto a carga anterior)
    2. Tasa de cambio entre cargas (% de registros nuevos/modificados/eliminados)
    3. Tiempo de procesamiento por etapa
    4. Alertas generadas (total, por fuente, score promedio)
    5. Score promedio de matches

Alertas operacionales (5 definidas):
    A1. FUENTE_NO_ACTUALIZADA: Una fuente no se descargó exitosamente en >N días
        → Canal: email a equipo de datos, Severity: HIGH
    A2. TASA_MATCHES_ANOMALA: La tasa de matches subió >50% respecto al baseline
        → Canal: Slack #compliance-alerts, Severity: CRITICAL
    A3. FUENTE_VACIA: Una fuente devolvió 0 registros tras ingesta
        → Canal: PagerDuty, Severity: CRITICAL
    A4. LATENCIA_ALTA: El pipeline tardó >2x el tiempo baseline
        → Canal: Slack #data-ops, Severity: MEDIUM
    A5. SCORE_PROMEDIO_BAJO: El score promedio de matches cayó por debajo de umbral
        (indica posible degradación del matching o cambios en la fuente)
        → Canal: email a data science, Severity: LOW
"""

from datetime import datetime
from typing import List, Dict, Optional

from pipeline.utils import get_logger




logger = get_logger("monitoring")


class MetricsStore:
    """Store en memoria para métricas del pipeline. En producción: Prometheus/InfluxDB"""

    def __init__(self):
        self._metrics: Dict = {}
        self._history: List[Dict] = []

    def record(self, key:str, value, tags:Dict=None): # pylint: disable=missing-function-docstring
        entry = {
            "key": key,
            "value": value,
            "tags": tags or {},
            "timestamp": datetime.now().isoformat(),
        }
        self._metrics[key] = entry
        self._history.append(entry)

    def get(self, key:str): # pylint: disable=missing-function-docstring
        entry = self._metrics.get(key)
        return entry["value"] if entry else None

    def get_history(self, key:str, last_n:int=10) -> List: # pylint: disable=missing-function-docstring
        return [e["value"] for e in self._history if e["key"] == key][-last_n:]

    def snapshot(self) -> Dict: # pylint: disable=missing-function-docstring
        return {k: v["value"] for k, v in self._metrics.items()}


class AlertRule:
    """Creación de reglas para las alertas"""
    def __init__(self, name: str, description: str, severity: str, channel: str, threshold):
        self.name = name
        self.description = description
        self.severity = severity
        self.channel = channel
        self.threshold = threshold

    def evaluate(self, _) -> Optional[Dict]: # pylint: disable=missing-function-docstring
        raise NotImplementedError

    def _fire(self, message:str, data:Dict=None) -> Dict:
        alert = {
            "rule": self.name,
            "description": self.description,
            "severity": self.severity,
            "channel": self.channel,
            "message": message,
            "data": data or {},
            "timestamp": datetime.now().isoformat(),
            "fired": True,
        }
        logger.warning("[ALERT][%s] %s: %s", self.severity, self.name, message)
        return alert


class FuenteNoActualizadaAlert(AlertRule):
    """A1: Fuente sin actualización en más de N días"""

    def __init__(self, max_days: int = 3):
        super().__init__(
            "FUENTE_NO_ACTUALIZADA",
            f"Una fuente no se actualizó en los últimos {max_days} días",
            severity="HIGH",
            channel="email:data-team@company.com",
            threshold=max_days,
        )

    def evaluate(self, metrics:MetricsStore) -> Optional[Dict]:
        last_run = metrics.get("last_successful_run_timestamp")
        if not last_run:
            return None
        try:
            last_dt = datetime.fromisoformat(last_run)
            days_ago = (datetime.now() - last_dt).days
            if days_ago > self.threshold:
                return self._fire(
                    f"Pipeline sin ejecución exitosa hace {days_ago} días",
                    {"days_since_last_run": days_ago}
                )
        except Exception: # pylint: disable=broad-exception-caught
            pass
        return None


class TasaMatchesAnomaliaAlert(AlertRule):
    """A2: Tasa de matches subió abruptamente (>50% del baseline)"""

    def __init__(self, threshold_pct: float = 0.50):
        super().__init__(
            "TASA_MATCHES_ANOMALA",
            "La tasa de matches aumentó abruptamente respecto al baseline",
            severity="CRITICAL",
            channel="slack:#compliance-alerts",
            threshold=threshold_pct,
        )

    def evaluate(self, metrics:MetricsStore) -> Optional[Dict]:
        history = metrics.get_history("match_rate", last_n=5)
        current = metrics.get("match_rate")
        if not history or current is None or len(history) < 2:
            return None
        baseline = sum(history[:-1]) / len(history[:-1])
        if baseline > 0 and (current - baseline) / baseline > self.threshold:
            return self._fire(
                f"Match rate actual {current:.2%} vs baseline {baseline:.2%}",
                {"current": current, "baseline": baseline}
            )
        return None


class FuenteVaciaAlert(AlertRule):
    """A3: Una fuente devolvió 0 registros"""

    def __init__(self):
        super().__init__(
            "FUENTE_VACIA",
            "Una fuente activa devolvió 0 registros tras la ingesta",
            severity="CRITICAL",
            channel="pagerduty:data-pipeline",
            threshold=0,
        )

    def evaluate(self, metrics:MetricsStore) -> Optional[Dict]:
        counts = metrics.get("records_per_source") or {}
        empty = [src for src, count in counts.items() if count == 0]
        if empty:
            return self._fire(
                f"Fuentes con 0 registros: {', '.join(empty)}",
                {"empty_sources": empty}
            )
        return None


class LatenciaAltaAlert(AlertRule):
    """A4: Pipeline tardó >2x el tiempo baseline"""

    def __init__(self, baseline_seconds: float = 300.0):
        super().__init__(
            "LATENCIA_ALTA",
            "El pipeline tardó más del doble del tiempo baseline",
            severity="MEDIUM",
            channel="slack:#data-ops",
            threshold=baseline_seconds,
        )

    def evaluate(self, metrics:MetricsStore) -> Optional[Dict]:
        duration = metrics.get("pipeline_duration_seconds")
        if duration is not None and duration > self.threshold * 2:
            return self._fire(
                f"Pipeline tardó {duration:.1f}s (baseline: {self.threshold:.1f}s)",
                {"duration": duration, "baseline": self.threshold}
            )
        return None


class ScorePromedioBajoAlert(AlertRule):
    """A5: Score promedio de matches cayó por debajo de umbral"""

    def __init__(self, min_score: float = 0.85):
        super().__init__(
            "SCORE_PROMEDIO_BAJO",
            "El score promedio de matches cayó por debajo del umbral mínimo",
            severity="LOW",
            channel="email:data-science@company.com",
            threshold=min_score,
        )

    def evaluate(self, metrics:MetricsStore) -> Optional[Dict]:
        avg_score = metrics.get("avg_match_score")
        if avg_score is not None and avg_score < self.threshold:
            return self._fire(
                f"Score promedio {avg_score:.3f} < umbral {self.threshold}",
                {"avg_score": avg_score, "threshold": self.threshold}
            )
        return None


class MonitoringSystem:
    """Sistema de monitoreo que coordina métricas y alertas"""

    def __init__(self):
        self.metrics = MetricsStore()
        self.alert_rules = [
            FuenteNoActualizadaAlert(max_days=3),
            TasaMatchesAnomaliaAlert(threshold_pct=0.50),
            FuenteVaciaAlert(),
            LatenciaAltaAlert(baseline_seconds=300.0),
            ScorePromedioBajoAlert(min_score=0.85),
        ]
        self._fired_alerts: List[Dict] = []

    def record_ingestion(self, ingestion_results:Dict) -> None:
        """Registra métricas de la fase de ingesta"""
        records_per_source = ingestion_results.get("records_per_source", {})
        self.metrics.record("records_per_source", records_per_source)
        self.metrics.record(
            "last_successful_run_timestamp",
            datetime.now().isoformat()
        )
        total = sum(records_per_source.values())
        self.metrics.record("total_records", total)
        self.metrics.record(
            "pipeline_duration_seconds",
            ingestion_results.get("duration_seconds", 0)
        )
        logger.info("Monitoring: ingesta registrada — %d registros totales", total)

    def record_matching(self, matching_results: Dict) -> None:
        """Registra métricas de la fase de matching."""
        total_terceros = matching_results.get("total_terceros", 1)
        total_alerts = matching_results.get("total_alertas", 0)
        match_rate = total_alerts / total_terceros if total_terceros > 0 else 0

        self.metrics.record("match_rate", match_rate)
        self.metrics.record("total_alerts", total_alerts)
        self.metrics.record(
            "avg_match_score",
            matching_results.get("distribucion_scores", {}).get("avg", 0)
        )
        logger.info("Monitoring: matching registrado — %s alertas (%.2f%% tasa)",
                    total_alerts, match_rate)

    def evaluate_alerts(self) -> List[Dict]:
        """Evalúa todas las reglas de alerta y retorna las disparadas."""
        fired = []
        for rule in self.alert_rules:
            try:
                result = rule.evaluate(self.metrics)
                if result:
                    fired.append(result)
                    self._fired_alerts.append(result)
            except Exception as e: # pylint: disable=broad-exception-caught
                logger.error("Error evaluando regla %s: %s", rule.name, e)
        logger.info("Alertas disparadas: %d de %d reglas", len(fired), len(self.alert_rules))
        return fired

    def get_lineage(self, alert: Dict, sanctions_lookup: Dict, terceros_lookup: Dict) -> Dict:
        """
        Construye el linaje de una alerta específica.
        Permite trazar desde la alerta hasta el registro original en la fuente.

        Linaje:
          alerta → id_tercero → tercero (base interna)
                 → id_registro → sancion canónica
                              → id_fuente_original → registro en XML/CSV original
                              → fuente → URL de descarga
        """
        source_urls = {
            "OFAC": "https://www.treasury.gov/ofac/downloads/sdn.xml",
            "UN": "https://scsanctions.un.org/resources/xml/en/consolidated.xml",
            "EU": "https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content", # pylint: disable=line-too-long
            "FCPA": '''https://efts.sec.gov/LATEST/search-index?q="fcpa"&forms=AP''',
            "PACO_DISC": "https://paco7public7info7prod.blob.core.windows.net/paco-pulic-info/antecedentes_SIRI_sanciones_Cleaned.zip", # pylint: disable=line-too-long
            "PACO_PENAL": "https://paco7public7info7prod.blob.core.windows.net/paco-pulic-info/sanciones_penales_FGN.csv", # pylint: disable=line-too-long
            "WORLD_BANK": "https://projects.worldbank.org/en/projects-operations/procurement/debarred-firms", # pylint: disable=line-too-long
        }

        sancion = sanctions_lookup.get(alert.get("id_registro"), {})
        tercero = terceros_lookup.get(alert.get("id_tercero"), {})

        return {
            "alerta": {
                "tipo_match": alert.get("tipo_match"),
                "score": alert.get("score_similitud"),
                "timestamp": alert.get("timestamp"),
            },
            "tercero": {
                "id": tercero.get("id_tercero"),
                "nombre": f"{tercero.get('nombres','')} {tercero.get('apellidos','')}".strip(),
                "documento": tercero.get("numero_documento"),
            },
            "sancion_canonica": {
                "id_registro": sancion.get("id_registro"),
                "nombres": sancion.get("nombres"),
                "fuente": sancion.get("fuente"),
                "id_fuente_original": sancion.get("id_fuente_original"),
            },
            "fuente_original": {
                "fuente": sancion.get("fuente"),
                "url": source_urls.get(sancion.get("fuente"), "desconocida"),
                "id_en_fuente": sancion.get("id_fuente_original"),
                "instruccion": (
                    f"Buscar el campo uid/DATAID/logicalId='{sancion.get('id_fuente_original')}' "
                    f"en el archivo descargado de {source_urls.get(sancion.get('fuente'), '?')}"
                ),
            },
        }

    def generate_report(self) -> Dict:
        """Generar reporte"""
        return {
            "timestamp": datetime.now().isoformat(),
            "metrics": self.metrics.snapshot(),
            "alerts_fired": len(self._fired_alerts),
            "alerts": self._fired_alerts,
            "alert_rules": [
                {
                    "name": r.name,
                    "description": r.description,
                    "severity": r.severity,
                    "channel": r.channel,
                    "threshold": r.threshold,
                }
                for r in self.alert_rules
            ],
        }
