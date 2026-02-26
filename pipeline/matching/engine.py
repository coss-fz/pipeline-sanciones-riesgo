"""
Motor de matching masivo para screening de sanciones

Algoritmo elegido: Jaro-Winkler + normalización agresiva de nombres
    - Levenshtein: penaliza igual transposiciones y sustituciones. Para nombres
    propios, una transposición es más probable que una sustitución aleatoria.
    Jaro-Winkler está diseñado específicamente para nombres cortos.
    - Fonético (Soundex/Metaphone): útil para inglés pero falla en nombres
    hispanos, árabes o chinos que son frecuentes en listas de sanciones.
    - Embeddings semánticos: demasiado costosos computacionalmente para matching
    masivo en tiempo real sin GPU; además no agregan valor para nombres propios
    donde la semántica es irrelevante.
    - Jaro-Winkler: O(n*m) por par pero con early termination. Favorece prefijos
    comunes (útil para nombres con partículas: "DE LA", "VAN DER"). Bien
    calibrado para errores tipográficos en nombres.

Escenario de falla de Jaro-Winkler:
    - Nombres muy cortos (2-3 letras): alta similitud espuria.
    - Nombres con diferente orden (apellido primero vs. nombre primero).
    - Transliteraciones (Mohamad / Mohamed / Muhammad).

Umbrales configurables:
    - THRESHOLD_HIGH: match seguro (no requiere revisión)
    - THRESHOLD_LOW: zona gris (requiere revisión humana)
    - Por debajo de THRESHOLD_LOW: descartado
"""

import time
import re
from datetime import datetime
from typing import List, Dict, Optional, Tuple

from pipeline.utils import get_logger, normalize_name_for_matching




logger = get_logger("matching.engine")


def _jaro(s1:str, s2:str) -> float:
    """Similitud de Jaro entre dos strings"""
    if s1 == s2:
        return 1.0
    l1, l2 = len(s1), len(s2)
    if l1 == 0 or l2 == 0:
        return 0.0

    match_dist = max(l1, l2) // 2 - 1
    if match_dist < 0:
        match_dist = 0

    s1_matches = [False] * l1
    s2_matches = [False] * l2
    matches = 0
    transpositions = 0

    for i in range(l1):
        start = max(0, i - match_dist)
        end = min(i + match_dist + 1, l2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(l1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    return (matches / l1 + matches / l2 + (matches - transpositions / 2) / matches) / 3


def jaro_winkler(s1:str, s2:str, p:float=0.1) -> float:
    """Similitud de Jaro-Winkler. 'p' es el factor de escala para prefijos (típico: 0.1)"""
    jaro_score = _jaro(s1, s2)
    prefix = 0
    for i in range(min(4, len(s1), len(s2))):
        if s1[i] == s2[i]:
            prefix += 1
        else:
            break
    return jaro_score + prefix * p * (1 - jaro_score)


def name_similarity(n1:Optional[str], n2:Optional[str]) -> float:
    """
    - Similitud entre dos nombres normalizados
    - Aplica Jaro-Winkler sobre la versión normalizada
    - También prueba inversión de palabras para manejar "Nombre Apellido" vs "Apellido Nombre"
    """
    if not n1 or not n2:
        return 0.0

    a = normalize_name_for_matching(n1) or ""
    b = normalize_name_for_matching(n2) or ""

    if not a or not b:
        return 0.0

    # Score directo
    score = jaro_winkler(a, b)

    # Probar inversión de palabras (maneja diferente orden)
    words_a = a.split()
    words_b = b.split()
    if len(words_a) > 1 or len(words_b) > 1:
        a_rev = " ".join(reversed(words_a))
        score_rev = jaro_winkler(a_rev, b)
        score = max(score, score_rev)

    # Probar nombre completo vs. nombre parcial (primer nombre + primer apellido)
    if len(words_a) >= 2 and len(words_b) >= 2:
        a_partial = f"{words_a[0]} {words_a[-1]}"
        b_partial = f"{words_b[0]} {words_b[-1]}"
        score_partial = jaro_winkler(a_partial, b_partial)
        score = max(score, score_partial * 0.95)  # leve penalización por match parcial

    return score




class MatchingEngine:
    """Motir para el matching"""
    def __init__(
        self,
        threshold_high:float=0.92,
        threshold_low:float=0.80,
    ):
        self.threshold_high = threshold_high
        self.threshold_low = threshold_low
        self._sanctions: List[Dict] = []
        self._doc_index: Dict[str, List[Dict]] = {}   # documento → registros
        self._alias_index: Dict[str, List[Dict]] = {} # alias normalizado → registros
        self._name_index: Dict[str, List[Dict]] = {}  # primera letra → registros (blocking)
        logger.info("MatchingEngine iniciado (high=%.2f, low=%.2f)", threshold_high, threshold_low)

    def load_sanctions(self, sanctions:List[Dict]) -> None:
        """
        - Carga la lista de sanciones y construye índices para matching eficiente
        - El blocking por iniciales reduce el espacio de comparación ~26x
        """
        self._sanctions = sanctions
        self._doc_index = {}
        self._alias_index = {}
        self._name_index = {}

        for rec in sanctions:
            # Índice por documento
            doc = rec.get("numero_documento")
            if doc:
                doc_clean = re.sub(r"[\s\-\.]", "", doc.upper())
                self._doc_index.setdefault(doc_clean, []).append(rec)

            # Índice por nombre (blocking por primera letra)
            nombres = rec.get("nombres") or ""
            if nombres:
                key = nombres[0].upper() if nombres else "_"
                self._name_index.setdefault(key, []).append(rec)

            # Índice por aliases
            for alias in rec.get("aliases", []):
                alias_norm = normalize_name_for_matching(alias) or ""
                if alias_norm:
                    self._alias_index.setdefault(alias_norm[:5], []).append(rec)

        logger.info("Índices construidos: %s docs, %s bloques de nombre, %s bloques de alias",
            len(self._doc_index), len(self._name_index), len(self._alias_index))

    def match_one(self, tercero:Dict) -> List[Dict]:
        """
        - Ejecuta todos los tipos de matching para un tercero
        - Retorna lista de alertas
        """
        alerts = []
        now = datetime.utcnow().isoformat()

        t_nombres = tercero.get("nombres", "") or ""
        t_apellidos = tercero.get("apellidos", "") or ""
        t_full = f"{t_nombres} {t_apellidos}".strip()
        t_doc = tercero.get("numero_documento")
        t_id = tercero["id_tercero"]

        # --- 1. Match exacto por documento ---
        if t_doc:
            doc_clean = re.sub(r"[\s\-\.]", "", t_doc.upper())
            for rec in self._doc_index.get(doc_clean, []):
                alerts.append({
                    "id_tercero": t_id,
                    "id_registro": rec["id_registro"],
                    "fuente": rec["fuente"],
                    "tipo_match": "EXACTO_DOCUMENTO",
                    "score_similitud": 1.0,
                    "nombre_tercero": t_full,
                    "nombre_lista": f"{rec.get('nombres','')} {rec.get('apellidos','')}".strip(),
                    "requiere_revision": False,
                    "timestamp": now,
                })

        # --- 2. Match exacto y fuzzy por nombre ---
        # Blocking: sólo comparar con registros cuya primera letra coincide
        initial = t_nombres[0].upper() if t_nombres else None
        candidates = []
        if initial:
            candidates.extend(self._name_index.get(initial, []))
            # También buscar con inicial del apellido por si está invertido
            if t_apellidos:
                a_initial = t_apellidos[0].upper()
                if a_initial != initial:
                    candidates.extend(self._name_index.get(a_initial, []))

        seen_ids = {a["id_registro"] for a in alerts}

        for rec in candidates:
            if rec["id_registro"] in seen_ids:
                continue

            r_full = f"{rec.get('nombres','')} {rec.get('apellidos','')}".strip()
            score = name_similarity(t_full, r_full)

            if score >= self.threshold_high:
                tipo = "EXACTO_NOMBRE" if score >= 0.99 else "FUZZY_NOMBRE"
                requiere = False
            elif score >= self.threshold_low:
                tipo = "FUZZY_NOMBRE"
                requiere = True  # zona gris
            else:
                continue

            alerts.append({
                "id_tercero": t_id,
                "id_registro": rec["id_registro"],
                "fuente": rec["fuente"],
                "tipo_match": tipo,
                "score_similitud": round(score, 4),
                "nombre_tercero": t_full,
                "nombre_lista": r_full,
                "requiere_revision": requiere,
                "timestamp": now,
            })
            seen_ids.add(rec["id_registro"])

        # --- 3. Match por alias ---
        t_norm = normalize_name_for_matching(t_full) or ""
        if t_norm:
            prefix5 = t_norm[:5]
            for rec in self._alias_index.get(prefix5, []):
                if rec["id_registro"] in seen_ids:
                    continue
                for alias in rec.get("aliases", []):
                    alias_norm = normalize_name_for_matching(alias) or ""
                    score = jaro_winkler(t_norm, alias_norm) if alias_norm else 0.0
                    if score >= self.threshold_high:
                        alerts.append({
                            "id_tercero": t_id,
                            "id_registro": rec["id_registro"],
                            "fuente": rec["fuente"],
                            "tipo_match": "ALIAS",
                            "score_similitud": round(score, 4),
                            "nombre_tercero": t_full,
                            "nombre_lista": alias,
                            "requiere_revision": score < 0.99,
                            "timestamp": now,
                        })
                        seen_ids.add(rec["id_registro"])
                        break

        return alerts

    def run_batch(self, terceros:List[Dict], report_every:int=1000) -> Tuple[List[Dict], Dict]:
        """
        - Ejecuta matching masivo sobre toda la base de terceros
        - Retorna (alertas, métricas)
        """
        all_alerts = []
        start = time.time()

        for i, tercero in enumerate(terceros):
            alerts = self.match_one(tercero)
            all_alerts.extend(alerts)
            if (i + 1) % report_every == 0:
                elapsed = time.time() - start
                rps = (i + 1) / elapsed if elapsed > 0 else 0
                logger.info(
                    "Matching: %s/%s procesados (%.0f reg/s), %s alertas",
                    i + 1, len(terceros), rps, len(all_alerts))

        elapsed = time.time() - start
        rps = len(terceros) / elapsed if elapsed > 0 else 0

        # Distribución de scores
        scores = [a["score_similitud"] for a in all_alerts]
        score_dist = {
            "min": min(scores) if scores else 0,
            "max": max(scores) if scores else 0,
            "avg": sum(scores) / len(scores) if scores else 0,
            "count_high": sum(1 for s in scores if s >= self.threshold_high),
            "count_gray": sum(1 for s in scores if self.threshold_low <= s < self.threshold_high),
        }

        metrics = {
            "total_terceros": len(terceros),
            "total_alertas": len(all_alerts),
            "tiempo_segundos": round(elapsed, 2),
            "registros_por_segundo": round(rps, 1),
            "distribucion_scores": score_dist,
            "threshold_high": self.threshold_high,
            "threshold_low": self.threshold_low,
        }

        logger.info(
            "Matching completo: %s terceros, %s alertas en %.1fs (%.0f reg/s)",
            len(terceros), len(all_alerts), elapsed, rps)
        return all_alerts, metrics


def compute_precision_recall(
    alerts: List[Dict],
    planted: List[Dict],
) -> Dict:
    """
    Calcula precision y recall sobre los registros plantados
    """
    planted_ids = {r["id_tercero"] for r in planted if r.get("es_match_plantado")}
    alerted_ids = {a["id_tercero"] for a in alerts}

    tp = len(planted_ids & alerted_ids)
    fp = len({a["id_tercero"] for a in alerts} - planted_ids)
    fn = len(planted_ids - alerted_ids)

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    missed = planted_ids - alerted_ids
    false_alarms = {a["id_tercero"] for a in alerts} - planted_ids

    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "missed_planted_ids": list(missed)[:10],  # muestra primeros 10
        "false_alarm_sample": list(false_alarms)[:10],
    }
