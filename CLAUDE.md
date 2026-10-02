# Big Data - lambda26

Workspace del curso **Big Data**, UPeU 2026-2. Combina documentación (MkDocs,
`docs/`) con código de referencia real (`pyspark/`): entorno Docker + Spark,
y las guías de sesión con sus notebooks correspondientes.

## Dónde está cada cosa

- `docs/silabo_bigdata_2026_2.md` — sílabo oficial **vigente** (no editar
  salvo pedido explícito). `docs/silabo_bigdata_2026_1.md` es la versión
  anterior, solo como referencia histórica.
- `docs/index.md` — página de inicio curada (propósito del curso, producto,
  arquitectura de `lambda26`, tabla de sesiones con hipervínculos).
- `docs/sesiones/S0X_*.md` — guías de sesión. Existen **S01-S07, S12 y S15**;
  el resto se construye progresivamente.
- `mkdocs.yml` — nav con "Inicio", "Guía de Proyecto Sello", "Silabos" y las
  unidades con sus sesiones (`docs/sesiones/S0X_*.md`).
- `pyspark/` — entorno Docker real del laboratorio (Dockerfile, `compose.yml`,
  `.env`/`.env.example`). La carpeta `pyspark/sesiones/` es la única que se
  monta dentro del contenedor (`./sesiones:/opt`) — los archivos de
  configuración (`Dockerfile`, `compose.yml`, `.env`, `README.md`) se quedan
  fuera a propósito, para no aparecer en el explorador de Jupyter.
- `pyspark/sesiones/sXX-nombre/` — una carpeta autocontenida por sesión con
  datos reales: el notebook, `data/` (dataset de entrada, si aplica) y
  `artifacts/` (salidas, si aplica). No hay carpetas `data/`/`notebooks/`
  compartidas entre sesiones.

## Convenciones

- No editar los archivos `silabo_bigdata_*.md` salvo que se pida
  explícitamente.
- **Flujo de trabajo para S03 en adelante: primero el notebook, después la
  sección 3 de la guía.** Construir y correr el notebook real
  (`pyspark/sesiones/sXX-nombre/`) primero, verificar que el código
  efectivamente funciona contra los datos reales, y recién ahí escribir
  la sección 3 ("Aplica") de la guía transcribiendo lo ya probado — no al
  revés. Motivo: en S02 se escribió la guía primero y el notebook después,
  y aparecieron varios problemas reales solo al ejecutar (rutas rotas tras
  reestructurar carpetas, `.describe()` ilegible con muchas columnas, un
  bug real de formato de fecha) que obligaron a corregir la guía ya
  publicada varias veces. Notebook primero evita ese ciclo.
  **Estado de S03:** verificada con corrida real completa sin errores
  (2026-08-21), pero **luego se reestructuró** (mismo día) para seguir la
  secuencia exacta de `Pyspark_Introduccion.pdf` (filtrado → orden →
  duplicados → nulos, en vez de nulos primero) e incorporar técnicas que
  faltaban: `filter()` con expresiones SQL, `where()`, filtrar nulos/texto,
  `orderBy()`/`sort()` con varias formas, `dropDuplicates()` completo y
  multi-columna, identificar duplicados con `groupBy()+count()`, marcar
  duplicados con `Window`+`row_number()` sobre `customer_id`, `fillna()`
  como alias de `.na.fill()`, y `.na.drop()` sin argumentos como contraste.
  Se agregaron además `.write.format()` (CSV/JSON/Parquet genérico),
  `repartition("columna")` (particionamiento lógico vs. físico),
  `coalesce(1)` como contraste directo de `repartition(4)`, y
  `persist(StorageLevel.MEMORY_AND_DISK)` junto a `cache()` (del PDF,
  secciones "5. Escritura de datos" y "6. Optimización de consultas").
  **La reestructura completa ya se corrió en el servidor (2026-08-21),
  36/36 celdas, cero errores** — incluida la confirmación real de
  `.na.drop()` sin argumentos (462 911 de 1 371 980 filas sobreviven, 66.3%
  descartado) y `PartitionFilters` con la sintaxis nueva `.write.format()`.
  S3 queda verificada de punta a punta.
- Cada guía de sesión (`S0X_*.md`) sigue la plantilla ya establecida en
  S01/S02: 1. Introducción (1.1-1.7) → 2. Explica (teoría genérica, sin
  atarse a datasets específicos) → 3. Aplica (práctica guiada, aquí sí con
  datos reales) → 4. Crea (autónoma, sobre el Proyecto Sello del equipo) →
  5. Cierre → Bibliografía. Figuras/Tablas con numeración continua por
  documento (APA 7).
- El dataset real de cada sesión (si pesa demasiado para git) se distribuye
  por Drive, no por Kaggle directo — ver el patrón de descarga en S02 (3.1).
- **Sincronización permanente notebook ↔ guía:** una vez que una sesión ya
  tiene notebook y guía publicados, cualquier corrección en uno (una ruta,
  un parámetro, un bloque nuevo) se refleja en el otro en el mismo cambio,
  sin excepción, en ambas direcciones.
- `pyspark/.gitignore` excluye `sesiones/*/data/` en general (no una entrada
  por sesión) — cualquier dataset nuevo bajo `sesiones/sXX-.../data/` ya
  queda afuera de git automáticamente, sin tocar el `.gitignore`.
