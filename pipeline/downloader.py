"""
Descarga robusta de fuentes con reintentos y manejo de errores
"""

import time
from typing import Optional, Dict
import requests

from pipeline.utils import get_logger




logger = get_logger("downloader")

DEFAULT_TIMEOUT = 60  # segundos
DEFAULT_RETRIES = 5
DEFAULT_BACKOFF = 2   # multiplicador exponencial




def download_files(
    url:str,
    retries:int=DEFAULT_RETRIES,
    timeout:int=DEFAULT_TIMEOUT,
    headers:Optional[Dict[str, str]]=None,
    backoff:float=DEFAULT_BACKOFF,
) -> Optional[bytes]:
    """
    - Descarga contenido binario de una URL con reintentos exponenciales
    - Retorna bytes o None si falla definitivamente
    """
    hdrs = {"user-agent": "xxxx (compliance-screening-pipeline)"}
    if headers:
        hdrs.update(headers)

    for attempt in range(1, retries + 1):
        try:
            logger.info("Descargando %s (intento %d/%d)", url, attempt, retries)
            resp = requests.get(url, headers=hdrs, timeout=timeout, stream=True)
            resp.raise_for_status()
            content = resp.content
            return content
        except requests.exceptions.HTTPError as e:
            logger.warning("HTTP %d en %s: %s", e.response.status_code, url, e)
            if e.response.status_code == 403:
                logger.warning("Se omite descarga: cliente no cuenta con permisos")
                break
        except requests.exceptions.ConnectionError as e:
            logger.warning("Error de conexión en %s: %s", url, e)
        except requests.exceptions.Timeout:
            logger.warning("Timeout en %s (intento %d)", url, attempt)
        except requests.exceptions.RequestException as e:
            logger.error("Error inesperado descargando %s: %s", url, e)
            break  # No reintentar errores no transitorios

        if attempt < retries:
            wait = backoff ** attempt
            logger.info("Reintentando en %.1fs…", wait)
            time.sleep(wait)

    logger.error("Fallo definitivo descargando %s", url)
    return None
