# Contrato de eventos — Proyecto Sello CitiBike (S07)

**Topic:** `citibike-eventos` · **Particiones:** 3 · **Consumer group:** `uso-citibike-group`

El topic recibe dos tipos de evento, distinguidos por `tipoEvento`. Cada uno tiene su propio esquema y su propio rango físico.

## 1. `bici.vibracion` (sensor real: acelerómetro del celular)

Ruta: celular (phyphox) → `lector_phyphox_mqtt.py` → MQTT `test.mosquitto.org` (`lambda26/citibike/<equipo>/vibracion`) → `bridge_mqtt_kafka.py` → Kafka.

```json
{
  "tipoEvento": "bici.vibracion",
  "bikeId": "bici-grimaldo-01",
  "accX": 0.41,
  "accY": -1.2,
  "accZ": 2.05,
  "magnitud": 2.41,
  "unidad": "m/s2",
  "origen": "phyphox",
  "timestamp": 1790903621779
}
```

| Campo | Tipo | Unidad | Descripción | Rango físico plausible |
|---|---|---|---|---|
| `tipoEvento` | string | — | Siempre `bici.vibracion` | — |
| `bikeId` | string | — | Id de la bicicleta. **Es la key de Kafka** | — |
| `accX`, `accY`, `accZ` | number | m/s² | Aceleración lineal (sin gravedad) por eje | −25 a 25 |
| `magnitud` | number | m/s² | √(accX²+accY²+accZ²) | 0 a 25 |
| `unidad` | string | — | Siempre `m/s2` | — |
| `origen` | string | — | `phyphox` | — |
| `timestamp` | integer | ms epoch | Momento de la lectura | — |

## 2. `estacion.estado` (fuente real: feed GBFS de Citi Bike)

Ruta: `producer_gbfs.py` → Kafka directo (corre en servidor, sí habla Kafka).

```json
{
  "tipoEvento": "estacion.estado",
  "stationId": "66db2fd0-0aca-11e7-82f6-3863bb44ef7c",
  "bicisDisponibles": 7,
  "docksDisponibles": 20,
  "origen": "gbfs",
  "timestamp": 1790903621779
}
```

| Campo | Tipo | Unidad | Descripción | Rango físico plausible |
|---|---|---|---|---|
| `tipoEvento` | string | — | Siempre `estacion.estado` | — |
| `stationId` | string | — | Id de estación. **Es la key de Kafka** | — |
| `bicisDisponibles` | integer | bicicletas | Bicis ancladas y disponibles | 0 a 150 |
| `docksDisponibles` | integer | anclajes | Anclajes libres | 0 a 150 |
| `origen` | string | — | `gbfs` | — |
| `timestamp` | integer | ms epoch | Momento de la ingesta | — |

## Justificación de los rangos

- **Vibración (25 m/s² ≈ 2,5 g):** pedaleando sobre pavimento, la aceleración lineal medida en el manubrio se mantiene en valores bajos; los baches fuertes generan picos breves. Una lectura por encima de ~2,5 g no corresponde a una bici en uso normal: indica un golpe/caída o un sensor mal fijado. Los acelerómetros de celular saturan muy por encima (±8 g o más), así que el límite es de plausibilidad operativa, no del hardware.
- **Estaciones (0–150):** un conteo no puede ser negativo, y ninguna estación tiene cientos de anclajes.

## Reglas de validación del consumer

| Resultado | Cuándo | `status` |
|---|---|---|
| Falla de esquema | No es JSON, `tipoEvento` desconocido, falta un campo o el tipo no coincide | `invalid` |
| Alerta de rango | Esquema correcto, pero algún valor fuera de su rango físico | `alerta` |
| OK | Pasa ambas validaciones | `consumed` |
