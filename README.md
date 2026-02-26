# pipeline-sanciones-riesgo
Pipeline que ingesta, normaliza y consolida fuentes de sanciones internacionales y nacionales, y ejecuta matching masivo contra una base sintética de 10.000 terceros para identificar alertas de riesgo.




## Estructura
```
pipeline-sanciones-riesgo/
├── pipeline/
│   ├── utils.py                    # Normalización, hashing, esquema canónico
│   ├── downloader.py               # Descarga con reintentos
│   ├── fuentes/
│   │   ├── ofac.py                 # Parser OFAC SDN XML
│   │   ├── un.py                   # Parser ONU Consolidated XML
│   │   ├── eu.py                   # Parser EU Financial Sanctions XML
│   │   ├── fcpa.py                 # Parser FCPA JSON paginado
│   │   ├── paco.py                 # Parser PACO Disc (ZIP/CSV) y Penal (CSV)
│   │   └── worldbank.py            # Scraper World Bank HTML/API
│   ├── normalizacion/
│   │   └── db.py                   # Capa de persistencia SQLite
│   ├── matching/
│   │   ├── engine.py               # Motor de matching Jaro-Winkler
│   │   └── synthetic.py            # Generador de base sintética
│   └── calidad/
│       ├── rules.py                # 8 reglas de calidad de datos
│       └── monitoring.py           # Sistema de monitoreo y alertas operacionales
├── data/
│   ├── terceros.csv                # Base sintética generada
│   ├── mapa_plantados.json         # Mapa de registros plantados para validación
│   ├── sanctions.db                # Base de Datos
│   └── raw/
├── reportes/                       # Generados al correr el pipeline
│   ├── reporte_ingesta.json
│   ├── reporte_alertas.json
│   ├── reporte_calidad.json
│   └── reporte_monitoreo.json
├── .gitignore
├── README.md
├── requirements.txt
└── run_pipeline.py                 # Entry point principal
```




## Configuración Inicial

### Ambiente Virtual
```bash
# Crear ambiente virtual
python -m venv .venv

# Activar ambiente virtual
source .venv/bin/activate # Windows -> .venv\Scripts\activate
```

### Instalación de librerías
```bash
pip install -r requirements.txt
```

## Uso

### Pipeline completo
```bash
python run_pipeline.py --env local
```

### Con caché (sin re-descargar fuentes)
```bash
python run_pipeline.py --env local --skip-download
```

### Solo fuentes específicas
```bash
python run_pipeline.py --env local --sources <f1,f2,f3,f4>
```

### Solo matching
```bash
python run_pipeline.py --env local --only-matching
```




## Fuentes
| # | Fuente | Entidad | Formato |
|---|--------|---------|---------|
| 1 | SDN List | OFAC — US Treasury | XML |
| 2 | UN Consolidated List | Consejo de Seguridad ONU | XML |
| 3 | EU Financial Sanctions | Unión Europea | XML |
| 4 | FCPA Enforcement Actions | DOJ / SEC | JSON paginado |
| 5 | Sanciones disciplinarias | Procuraduría — PACO | ZIP/CSV |
| 6 | Sanciones penales | Fiscalía — PACO | CSV |
| 7 | Debarred Firms | Banco Mundial | HTML paginado / API JSON |




## Esquema canónico
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_registro` | TEXT PK | SHA-256(fuente+id_original+nombre)[:16] |
| `fuente` | TEXT | OFAC \| UN \| EU \| FCPA \| PACO_DISC \| PACO_PENAL \| WORLD_BANK |
| `tipo_sujeto` | TEXT | PERSONA_NATURAL \| PERSONA_JURIDICA |
| `nombres` | TEXT | Normalizado (sin tildes, mayúsculas) |
| `apellidos` | TEXT | Normalizado |
| `aliases` | JSON | Lista de nombres alternativos |
| `fecha_nacimiento` | TEXT | ISO 8601 |
| `nacionalidad` | JSON | Lista de códigos ISO 3166-1 alpha-2 |
| `numero_documento` | TEXT | Pasaporte, cédula u otro ID |
| `tipo_sancion` | TEXT | Descripción del programa/sanción |
| `fecha_sancion` | TEXT | ISO 8601 |
| `fecha_vencimiento` | TEXT | ISO 8601 o NULL |
| `activo` | INT | 0=inactivo, 1=vigente |
| `fecha_ingesta` | TEXT | Timestamp UTC ISO 8601 |
| `hash_contenido` | TEXT | SHA-256 para detección de cambios |
| `id_fuente_original` | TEXT | UID/DATAID/logicalId en la fuente |




## Motor de matching

**Algoritmo**: Jaro-Winkler (implementado sin dependencias externas)

**Tipos de match**:
- `EXACTO_DOCUMENTO`: coincidencia por número de documento
- `EXACTO_NOMBRE`: score ≥ 0.99
- `FUZZY_NOMBRE`: score ≥ 0.92
- `ALIAS`: coincidencia contra alias conocido del sujeto

**Zona gris**: scores entre 0.80–0.92 generan alerta con `requiere_revision=True`




## Reglas de calidad (8 implementadas)
1. `fuente_no_nula` — todo registro debe tener fuente
2. `fuente_con_registros` — ninguna fuente activa puede tener 0 registros
3. `aliases_como_lista` — aliases debe ser lista JSON, no string
4. `fecha_sancion_no_futura` — fecha_sancion ≤ hoy
5. `hash_unico_por_fuente` — sin duplicados por (fuente, id_fuente_original)
6. `alerta_referencias_validas` — alertas referencian entidades existentes
7. `nombres_no_vacios` — al menos nombres o apellidos presente
8. `activo_es_booleano` — activo ∈ {0, 1}




## Estrategia de actualización
| Fuente | Frecuencia estimada | Método de detección |
|--------|--------------------|--------------------|
| OFAC | Varias veces/día | Hash del archivo (`.sig`) |
| UN | Semanal | Tamaño/ETag del XML |
| EU | Diaria | Fecha de publicación en header HTTP |
| FCPA | Variable | Comparar `total` de la API |
| PACO | Mensual | Tamaño del ZIP |
| World Bank | Mensual | Hash del JSON API |

**Re-ejecución incremental**: el campo `hash_contenido` detecta cambios registro a registro.
Solo los registros que cambiaron generan nueva actividad de matching.

**Notificación**: cuando un tercero previamente limpio aparece en una lista, se genera
una alerta de tipo `NUEVO_MATCH` y se notifica vía el canal configurado en el MonitoringSystem.




## Reportes generados
- `reportes/reporte_ingesta.json` — registros por fuente, nuevos/modificados/eliminados, tiempos
- `reportes/reporte_alertas.json` — alertas de matching con métricas de precision/recall
- `reportes/reporte_calidad.json` — resultados de las 8 reglas de calidad
- `reportes/reporte_monitoreo.json` — métricas operacionales y alertas disparadas
- `reportes/alertas.csv` — todas las alertas en formato tabular




## Linaje de alertas
Una alerta se puede trazar hacia atrás:
```
alerta.id_tercero → tabla terceros → datos del tercero en la base interna
alerta.id_registro → tabla sanctions → sanctions.id_fuente_original → registro original
sanctions.fuente → URL de descarga original → archivo raw en data/raw/
```
