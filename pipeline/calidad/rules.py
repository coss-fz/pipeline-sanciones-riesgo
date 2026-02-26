"""
Reglas de calidad de datos con justificación

Se implementan 8 reglas:
    1. fuente_no_nula: Ningún registro puede tener fuente nula
    2. fuente_con_registros: Ninguna fuente activa puede tener 0 registros post-ingesta
    3. aliases_como_lista: Los aliases deben ser listas JSON válidas, no strings planos
    4. fecha_sancion_no_futura: fecha_sancion no puede ser posterior a la fecha actual
    5. hash_unico_por_fuente: hash_contenido debe ser único por (fuente, id_fuente_original)
    6. alerta_referencias_validas: Toda alerta de matching debe referenciar
    7. nombres_no_vacios: Ningún registro puede tener nombres Y apellidos ambos nulos
    8. activo_es_booleano: El campo activo debe ser 0 o 1 (no NULL ni otros valores)
"""

from typing import List, Dict, Tuple
from datetime import datetime

from pipeline.utils import get_logger




logger = get_logger("calidad.rules")


class QualityRule:
    """Definición de una regla"""
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description

    def check(self, _, __) -> Tuple[bool, List[str]]: # pylint: disable=missing-function-docstring
        raise NotImplementedError


class FuenteNoNula(QualityRule):
    """Regla 1: fuente nunca nula"""
    def __init__(self):
        super().__init__("fuente_no_nula", "Todo registro debe tener fuente definida")

    def check(self, records, _):
        errors = [
            f"Registro {r.get('id_registro','?')} tiene fuente nula"
            for r in records if not r.get("fuente")
        ]
        return len(errors) == 0, errors


class FuenteConRegistros(QualityRule):
    """Regla 2: cada fuente debe tener al menos 1 registro"""
    def __init__(self, expected_sources: List[str]):
        super().__init__("fuente_con_registros", "Cada fuente activa debe tener >0 registros")
        self.expected_sources = expected_sources

    def check(self, records, _):
        counts = {}
        for r in records:
            f = r.get("fuente")
            if f:
                counts[f] = counts.get(f, 0) + 1
        errors = [
            f"Fuente '{src}' tiene 0 registros (posible fallo de ingesta)"
            for src in self.expected_sources
            if counts.get(src, 0) == 0
        ]
        return len(errors) == 0, errors


class AliasesComoLista(QualityRule):
    """Regla 3: aliases deben ser lista, no string"""
    def __init__(self):
        super().__init__("aliases_como_lista", "aliases debe ser lista JSON válida")

    def check(self, records, _):
        errors = []
        for r in records:
            aliases = r.get("aliases")
            if aliases is not None and not isinstance(aliases, list):
                errors.append(
                    f"Registro {r.get('id_registro','?')} tiene aliases \
                        como {type(aliases).__name__}: {aliases!r}"
                )
        return len(errors) == 0, errors[:20]  # limitar output


class FechaSancionNoFutura(QualityRule):
    """Regla 4: fecha_sancion no puede ser futura"""
    def __init__(self):
        super().__init__("fecha_sancion_no_futura", "fecha_sancion no puede ser posterior a hoy")

    def check(self, records, _):
        today = datetime.now().date().isoformat()
        errors = [
            f"Registro {r.get('id_registro','?')} ({r.get('fuente')}) "
            f"tiene fecha_sancion futura: {r.get('fecha_sancion')}"
            for r in records
            if r.get("fecha_sancion") and r["fecha_sancion"] > today
        ]
        return len(errors) == 0, errors[:20]


class HashUnicoPorFuente(QualityRule):
    """Regla 5: hash_contenido único por (fuente, id_fuente_original)"""
    def __init__(self):
        super().__init__(
            "hash_unico_por_fuente",
            "hash_contenido debe ser único por (fuente, id_fuente_original)"
        )

    def check(self, records, _):
        seen = {}
        errors = []
        for r in records:
            key = (r.get("fuente"), r.get("id_fuente_original"))
            h = r.get("hash_contenido")
            if key in seen and seen[key] != h:
                errors.append(
                    f"Duplicado hash para ({key[0]}, {key[1]}): "
                    f"{seen[key]} vs {h}"
                )
            seen[key] = h
        return len(errors) == 0, errors[:20]


class AlertaReferenciasValidas(QualityRule):
    """Regla 6: alertas deben referenciar ids existentes"""
    def __init__(self):
        super().__init__(
            "alerta_referencias_validas",
            "Toda alerta debe referenciar id_tercero e id_registro existentes"
        )

    def check(self, records, context=None):
        if not context:
            return True, []
        alert_rows = context.get("alerts", [])
        valid_terceros = {r["id_tercero"] for r in context.get("terceros", [])}
        valid_sanciones = {r["id_registro"] for r in records}
        errors = []
        for a in alert_rows:
            if a.get("id_tercero") not in valid_terceros:
                errors.append(f"Alerta referencia id_tercero inexistente: {a.get('id_tercero')}")
            if a.get("id_registro") not in valid_sanciones:
                errors.append(f"Alerta referencia id_registro inexistente: {a.get('id_registro')}")
        return len(errors) == 0, errors[:20]


class NombresNoVacios(QualityRule):
    """Regla 7: al menos nombres o apellidos debe estar poblado"""
    def __init__(self):
        super().__init__(
            "nombres_no_vacios", "Todo registro debe tener al menos nombres o apellidos"
        )

    def check(self, records, _):
        errors = [
            f"Registro {r.get('id_registro','?')} ({r.get('fuente')}) "
            f"no tiene nombres ni apellidos"
            for r in records
            if not r.get("nombres") and not r.get("apellidos")
        ]
        return len(errors) == 0, errors[:20]


class ActivoEsBooleano(QualityRule):
    """Regla 8: campo activo debe ser 0 o 1"""
    def __init__(self):
        super().__init__("activo_es_booleano", "Campo activo debe ser 0 o 1, no nulo")

    def check(self, records, _):
        errors = [
            f"Registro {r.get('id_registro','?')} tiene activo={r.get('activo')!r}"
            for r in records
            if r.get("activo") not in (0, 1, True, False)
        ]
        return len(errors) == 0, errors[:20]


EXPECTED_SOURCES = ["OFAC", "UN", "EU", "FCPA", "PACO_DISC", "PACO_PENAL", "WORLD_BANK"]


def run_quality(records: List[Dict], context: Dict = None) -> Dict:
    """
    - Ejecuta todas las reglas de calidad sobre los registros normalizados
    - Retorna reporte con resultados por regla
    """
    rules = [
        FuenteNoNula(),
        FuenteConRegistros(EXPECTED_SOURCES),
        AliasesComoLista(),
        FechaSancionNoFutura(),
        HashUnicoPorFuente(),
        AlertaReferenciasValidas(),
        NombresNoVacios(),
        ActivoEsBooleano(),
    ]

    report = {
        "timestamp": datetime.now().isoformat(),
        "total_records": len(records),
        "rules": [],
        "overall_passed": True,
    }

    for rule in rules:
        passed, errors = rule.check(records, context)
        result = {
            "rule": rule.name,
            "description": rule.description,
            "passed": passed,
            "error_count": len(errors),
            "errors_sample": errors[:5],
        }
        report["rules"].append(result)
        if not passed:
            report["overall_passed"] = False
            logger.warning("Regla FALLIDA: %s — %d errores", rule.name, len(errors))
        else:
            logger.info("Regla OK: %s", rule.name)

    total_errors = sum(r["error_count"] for r in report["rules"])
    report["total_errors"] = total_errors
    logger.info("Calidad: %s (%s errores en %s reglas)",
        "PASSED" if report["overall_passed"] else "FAILED", total_errors, len(rules))
    return report
