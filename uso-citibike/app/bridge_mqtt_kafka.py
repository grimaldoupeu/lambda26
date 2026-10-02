"""
Puente MQTT -> Kafka (mismo patron que 3.7).

Se suscribe al topic MQTT del Proyecto Sello y reenvia cada payload a Kafka
TAL CUAL llego: sin validar ni transformar. La key de Kafka es el id del
dispositivo (bikeId), asi cada bicicleta conserva su orden dentro de su
particion.
"""
import json
import os
import uuid

import paho.mqtt.client as mqtt
from kafka import KafkaProducer

MQTT_HOST = os.getenv("MQTT_HOST", "test.mosquitto.org")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "lambda26/citibike/equipo01/vibracion")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_CITIBIKE", "citibike-eventos")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: value,  # bytes crudos, sin tocar
)


def log(**campos):
    print(json.dumps({"service": "uso-citibike", "component": "bridge", **campos}))


def extraer_key(raw):
    try:
        evento = json.loads(raw.decode("utf-8"))
        return evento.get("bikeId") or evento.get("stationId") or "desconocido"
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
        return "desconocido"


def on_connect(client, userdata, flags, reason_code, properties=None):
    log(mqttHost=MQTT_HOST, mqttTopic=MQTT_TOPIC,
        status="connected" if reason_code == 0 else f"connect_failed:{reason_code}")
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    key = extraer_key(msg.payload)
    metadata = producer.send(KAFKA_TOPIC, key=key, value=msg.payload).get(timeout=10)
    log(mqttTopic=msg.topic, kafkaTopic=metadata.topic, partition=metadata.partition,
        offset=metadata.offset, key=key, status="forwarded")


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                     client_id=f"uso-citibike-bridge-{uuid.uuid4().hex[:8]}")
client.on_connect = on_connect
client.on_message = on_message

log(mqttHost=MQTT_HOST, mqttPort=MQTT_PORT, kafkaBootstrapServers=KAFKA_BOOTSTRAP_SERVERS,
    kafkaTopic=KAFKA_TOPIC, status="starting")
client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
client.loop_forever()
