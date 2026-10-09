"""
Segunda fuente real (opcional): feed en vivo de Citi Bike (GBFS station_status).

Los numeros de bicis/docks disponibles salen de los sensores fisicos de los
anclajes de cada estacion. Publica directo a Kafka (este script corre en un
servidor, si puede hablar Kafka; no necesita MQTT), con stationId como key:
con varias estaciones se ve la distribucion real entre particiones.
"""
import json
import os
import time
import urllib.request

from kafka import KafkaProducer

GBFS_URL = os.getenv("GBFS_URL", "https://gbfs.citibikenyc.com/gbfs/en/station_status.json")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_CITIBIKE", "citibike-eventos")
NUM_ESTACIONES = int(os.getenv("GBFS_NUM_ESTACIONES", "6"))
INTERVAL_S = int(os.getenv("GBFS_INTERVAL_S", "30"))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def log(**campos):
    print(json.dumps({"service": "uso-citibike", "component": "producer-gbfs", **campos}))


def leer_feed():
    req = urllib.request.Request(GBFS_URL, headers={"User-Agent": "lambda26-uso-citibike"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


estaciones_fijas = None
log(gbfsUrl=GBFS_URL, kafkaTopic=KAFKA_TOPIC, numEstaciones=NUM_ESTACIONES, status="starting")

while True:
    try:
        feed = leer_feed()
    except Exception as ex:
        log(status="gbfs_unreachable", error=str(ex))
        time.sleep(INTERVAL_S)
        continue

    estaciones = feed.get("data", {}).get("stations", [])
    if estaciones_fijas is None:  # se fijan las primeras N en servicio para seguirlas en el tiempo
        # S08: el feed lista primero estaciones dadas de baja (is_installed=0, siempre 0 bicis y 0 docks)
        en_servicio = [e for e in estaciones if e.get("is_installed") == 1 and e.get("is_renting") == 1]
        estaciones_fijas = [e["station_id"] for e in en_servicio[:NUM_ESTACIONES]]
    por_id = {e.get("station_id"): e for e in estaciones}

    for station_id in estaciones_fijas:
        e = por_id.get(station_id, {})
        evento = {
            "tipoEvento": "estacion.estado",
            "stationId": station_id,
            "bicisDisponibles": e.get("num_bikes_available"),
            "docksDisponibles": e.get("num_docks_available"),
            "origen": "gbfs",
            "timestamp": int(time.time() * 1000),
        }
        md = producer.send(KAFKA_TOPIC, key=station_id, value=evento).get(timeout=10)
        log(kafkaTopic=md.topic, partition=md.partition, offset=md.offset, stationId=station_id,
            bicisDisponibles=evento["bicisDisponibles"], status="published")

    time.sleep(INTERVAL_S)
