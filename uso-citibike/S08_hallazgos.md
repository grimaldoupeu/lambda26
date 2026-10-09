# S08 — Errores y hallazgos reales (streaming Spark sobre `citibike-eventos`)

Registro de lo que realmente falló o sorprendió al construir y ejecutar
`pyspark/sesiones/s08-streaming-estructurado/08_streaming_citibike_autonoma.ipynb`
(8 de octubre de 2026, hora de Lima). Sirve para la sección "Error o hallazgo" del informe.

## 1. La IP del celular cambió entre S07 y S08

- **Síntoma:** `uso-citibike/compose.yml` todavía apuntaba a `PHYPHOX_URL=http://112.138.0.105:8080` (S07). Con esa IP, el lector habría registrado `phyphox_unreachable`.
- **Diagnóstico:** el celular recibe la IP por DHCP del wifi y cambia entre sesiones. Probé `/config` en las dos candidatas: `http://112.138.0.106:8080/config` devolvió el JSON del experimento *Aceleración (sin g)* y `http://10.181.117.198:8080` dio timeout.
- **Solución:** actualicé la IP a `112.138.0.106` en `uso-citibike/compose.yml` y en el valor por defecto de `uso-citibike/app/lector_phyphox_mqtt.py`, y recreé el contenedor (`docker compose ... up -d`). Ojo: una variable de `environment` nueva solo se aplica al **recrear** el contenedor, no con `docker restart`.
- **Lección:** la IP del sensor es configuración de entorno, no código. Hay que verificarla al inicio de cada sesión.

## 2. Las lecturas de S07 ya no estaban en Kafka (retención de 7 días)

- **Síntoma:** el plan era reenviar como dato tardío una lectura real de la corrida de S07 (1 de octubre de 2026, `timestamp` ≈ 1790904800000). La lectura batch de `citibike-eventos` desde `earliest` **solo devolvió datos de hoy**: las particiones empezaban en los offsets 145, 1704 y 144, no en 0.
- **Diagnóstico:** `kafka-configs.sh --describe` muestra `retention.ms=604800000` (7 días, el valor por defecto). La corrida de S07 terminó el 1 de octubre ~20:30 (Lima) y la de S08 empezó el 8 de octubre ~22:50: más de 7 días después, así que el broker borró esos segmentos. Detalle curioso: al arrancar hoy, el consumer `uso-citibike-group` alcanzó a leer los últimos mensajes de S07 que aún quedaban (offsets 1698-1703 de la partición 1, con `latencyMs` ≈ 611 millones de ms ≈ 7,07 días) y poco después el broker los borró. Su log solo guarda `magnitud`, no `accX/accY/accZ`, así que no se podía reconstruir ese JSON sin inventar valores. (Copia local de esa evidencia: `uso-citibike/app/logs/consumer_s08_arranque.log`, no versionada.)
- **Solución:** el evento tardío es la **lectura real más antigua de `bici-grimaldo-01` que el topic todavía conserva**. Se reenvía con los mismos bytes de `key` y `value`, sin modificar, y llega decenas de minutos tarde, muy por detrás del watermark de 20 s.
- **Lección:** Kafka no es un archivo histórico. Lo que se quiera conservar más allá de `retention.ms` hay que persistirlo, que es justo lo que hace la sección 6 con Parquet.

## 3. `pyspark/.env` no existía: Jupyter arrancaba sin token

- **Síntoma:** `pyspark/.env` no existía (está en `.gitignore` y no viene en el clon), así que `JUPYTER_TOKEN` quedaba vacío y la URL documentada `?token=sintoken` no correspondía.
- **Solución:** `cp pyspark/.env.example pyspark/.env` (token `sintoken`) y recrear el contenedor.

## 4. `compose.kafka.yml` no existía y la guía 3.1 no trae su contenido

- **Síntoma:** sin el override, el contenedor de PySpark no está en la red `lambda26-kafka-net` y `kafka:9092` no resuelve.
- **Solución:** creé `pyspark/compose.kafka.yml` con la red externa `lambda26-kafka-net` (mismo patrón que `uso-citibike/compose.yml`). Verificación: `kafka` resuelve a `172.23.0.2` desde `lambda26-pyspark`.

## 5. Spark muestra horas UTC, no las de Lima

- **Síntoma:** el contenedor corre en UTC (`date` → `Fri Oct 9 03:53 UTC`). Las ventanas aparecían con fecha del 9 de octubre y 5 horas de adelanto respecto del reloj de la laptop de las capturas, lo que haría dudar de la coherencia de fechas.
- **Solución:** `.config("spark.sql.session.timeZone", "America/Lima")` en la `SparkSession`. `lastProgress["eventTime"]` sigue reportando en UTC (con `Z`), y así se indica en el notebook.

## 6. El indicador de utilización salía NULL: el productor GBFS seguía estaciones dadas de baja

- **Síntoma:** en la primera ejecución completa del notebook, la ventana deslizante de estaciones (sección 2.2) mostró `bicis_prom = 0.0`, `docks_prom = 0.0` y `ocupacion_pct = NULL` para **las 6 estaciones**. El resto de las secciones funcionó.
- **Diagnóstico:** `producer_gbfs.py` fija las **primeras 6** estaciones del feed `station_status.json` sin mirar su estado. Al consultar el feed (2519 estaciones, 2456 instaladas), las primeras de la lista tenían `is_installed = 0` e `is_renting = 0`: estaciones dadas de baja que siempre reportan 0 bicis y 0 anclajes. En S07 el orden del feed era otro y las 6 elegidas sí estaban activas (por ejemplo, `2177014969129222184` con 11 bicis). El `NULL` no era un error de Spark: `try_divide(0, 0 + 0)` devuelve `NULL` en vez de lanzar la división por cero de ANSI.
- **Solución:** el productor ahora fija las primeras 6 estaciones con `is_installed == 1` e `is_renting == 1`. Reinicié el contenedor, relancé los 4 scripts y volví a ejecutar el notebook completo. La ocupación real ahora va de 25,4 % a 100 %.
- **Lección:** que el esquema sea válido no significa que el dato sirva. El consumer de S07 marcaba estos eventos como `consumed` (0 está dentro del rango 0-150), y solo una métrica agregada con sentido de negocio dejó ver el problema.

## 7. Precaución de diseño: un checkpoint reutilizado ignora `startingOffsets`

- Al volver a ejecutar el notebook, una consulta que apunta a un `checkpointLocation` ya existente retoma desde los offsets guardados en `offsets/` e ignora `startingOffsets="latest"`. Por eso cada sección borra su checkpoint antes de arrancar. En el Parquet de la sección 6 también se borra al inicio de la sección, para que la corrida 1 parta de cero en cada ejecución.

## Resultados de la corrida final (8 de octubre de 2026, ~23:10 Lima)

| Sección | Resultado real |
|---|---|
| 0. Lectura batch | 1137 mensajes; partición 1 con `bici-grimaldo-01` y las demás con `stationId` |
| 2.1 Ventana 10 s + watermark 20 s | 8 micro-lotes en 40 s, ~10 lecturas por ventana |
| 2.2 Deslizante 2 min / 30 s | 12 micro-lotes en 65 s, ocupación real entre 25,4 % y 100 % |
| 3. Dato tardío | lectura real `1791517959106` (22:52:39 Lima, ~20 min tarde): 0 filas en la salida |
| 4. Duplicado | 3 copias en Kafka → 1 en la salida deduplicada |
| 5. Disparo | 1 s → batchId 12; 6 s → batchId 2 (15 s cada uno) |
| 5. Checkpoint | `offsets/7` → `{"citibike-eventos":{"0":223,"1":2767,"2":183}}` |
| 6. Parquet | corrida 1 = 38 filas, corrida 2 = 81 filas (acumula) |
