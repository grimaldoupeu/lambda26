# uso-atmos

Ingesta de telemetría IoT (ESP32: temperatura, humedad, presión) hacia
Kafka — la fuente principal de datos para la inferencia de series de tiempo
de S10 (S07). Tiene dos caminos, con el mismo contrato de evento y el mismo
consumer para ambos:

- **Simulación rápida en Python** (`producer_sensores.py`/`consumer_sensores.py`):
  sin hardware, para verificar el patrón productor-consumidor y el
  particionado por dispositivo.
- **Dispositivo real simulado en Wokwi** (`wokwi/`): un ESP32 con firmware
  Arduino real, sensor DHT22 virtual (temperatura/humedad) más un
  potenciómetro (presión — Wokwi no tiene una pieza de presión barométrica
  nativa), publicando por MQTT a través de un puente (`bridge_mqtt_kafka.py`)
  hacia el mismo topic `atmos-eventos`.

## Requisitos

- Kafka corriendo (`kafka/`, ver su propio README) — este contenedor se une a
  la red externa `lambda26-kafka-net`, así que falla al levantarse si Kafka
  no está arriba primero.

## Uso

Desde esta carpeta:

```powershell
docker compose up -d --build
```

Contenedor esperado:

```powershell
docker compose ps
```

```text
lambda26-uso-atmos
```

Entrar al contenedor:

```powershell
docker compose exec uso-atmos sh
```

Ejecutar el consumer primero (queda escuchando, no termina solo — usa
`Ctrl+C` para salir):

```bash
python /app/consumer_sensores.py
```

En **otra terminal**, ejecutar el producer (simula 3 sensores ESP32
publicando una lectura cada uno por ronda, en bucle infinito — también se
detiene con `Ctrl+C`):

```powershell
docker compose exec uso-atmos python /app/producer_sensores.py
```

## Qué esperar

El consumer imprime una línea JSON por cada lectura recibida, con
`topic`/`partition`/`offset`/`key` reales, `latencyMs` (tiempo entre que el
producer publicó y el consumer procesó) y `status: "consumed"`. Los tres
sensores simulados (`esp32-patio`, `esp32-invernadero`, `esp32-laboratorio`)
publican con su propio `sensorId` como **key** de Kafka — eso mantiene el
orden de las lecturas de un mismo sensor dentro de la misma partición. Las
lecturas no son números independientes en cada ronda: cada sensor avanza un
poco desde su valor anterior (caminata aleatoria acotada a un rango físico
realista), como lo haría un sensor real.

Si le llega algo que no es JSON válido o le falta un campo del contrato
(`tipoEvento`/`sensorId`/`temperatura`/`humedad`/`presion`/`timestamp`), no se
cae: lo marca `status: "invalid"` y expone `rawPayload`/`decodeError` para
diagnosticar — pruébalo publicando texto plano desde Kafka UI
(`http://localhost:48085`) al topic `atmos-eventos` mientras el consumer
sigue corriendo.

## Configuración

| Variable de entorno | Valor por defecto | Aplica a |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | producer, consumer y bridge |
| `KAFKA_TOPIC_ATMOS` | `atmos-eventos` | producer, consumer y bridge |
| `SENSOR_INTERVAL_MS` | `3000` | producer — pausa entre rondas de lectura |
| `SENSOR_IDS` | `esp32-patio,esp32-invernadero,esp32-laboratorio` | producer — lista separada por comas, ajusta el volumen simulado |
| `KAFKA_GROUP_ID` | `uso-atmos-group` | consumer |
| `MQTT_HOST` | `test.mosquitto.org` | bridge |
| `MQTT_PORT` | `1883` | bridge |
| `MQTT_USERNAME` | *(vacío, no hace falta en el broker público)* | bridge |
| `MQTT_PASSWORD` | *(vacío, no hace falta en el broker público)* | bridge |
| `MQTT_TOPIC` | `lambda26/atmos/equipo01/lecturas` | bridge |

## Conexión (simulación Python)

- broker interno: `kafka:9092`
- red Docker: `lambda26-kafka-net`

## Dispositivo real simulado (Wokwi + MQTT)

Un ESP32 no habla el protocolo de Kafka — ningún firmware Arduino/MicroPython
trae un cliente Kafka. El patrón real de IoT es dos saltos: el dispositivo
publica por **MQTT** (protocolo liviano, pensado para hardware), y un puente
se suscribe a ese topic MQTT y reenvía cada mensaje a Kafka tal cual llegó,
sin validarlo — la validación de esquema y de rango físico sigue viviendo en
un solo lugar: `consumer_sensores.py`, el mismo de la simulación Python.

El ESP32 simulado corre por completo en tu navegador (la CPU del ESP32 se
emula en WebAssembly, del lado del cliente, no en un servidor de Wokwi) —
pero sigue sin poder llegar a tu `localhost`: su WiFi simulado solo sale a
internet real. En vez de levantar un broker propio y exponerlo a
internet con un túnel (ngrok exige tarjeta de crédito verificada para túneles
TCP incluso en cuenta gratis), tanto el ESP32 como el bridge se conectan,
cada uno por su cuenta, a un broker que **ya es público**:
`test.mosquitto.org` — sin cuenta, sin túnel, sin tarjeta.

Como es un broker compartido por cualquiera en internet, el topic incluye un
identificador de equipo (`equipo01` por defecto) para no mezclar tus
mensajes con los de otro equipo del curso — cámbialo por el tuyo, tanto en
`MQTT_TOPIC` (bridge) como en `sketch.ino` (Wokwi, ver `wokwi/README.md`).

### 1. Levantar el bridge

```powershell
docker compose up -d --build
```

```powershell
docker compose exec uso-atmos python /app/bridge_mqtt_kafka.py
```

Debes ver `status: "connected"` contra `test.mosquitto.org`.

### 2. Simular el ESP32 en Wokwi

Ver `wokwi/README.md` para el firmware completo (ESP32 + DHT22 + potenciómetro) y
los pasos para correrlo en [wokwi.com](https://wokwi.com).

### Qué esperar

El log del bridge muestra `status: "connected"` al conectar al broker
público, y `status: "forwarded"` por cada lectura reenviada a Kafka, con el
`partition` y `offset` reales. Del otro lado, `consumer_sensores.py` (3.4)
procesa estas lecturas exactamente igual que las del simulador Python —
mismo esquema, misma validación de rango — sin ningún cambio de código: para
Kafka, no hay diferencia entre un evento que vino de un script y uno que
vino de un sensor real.
