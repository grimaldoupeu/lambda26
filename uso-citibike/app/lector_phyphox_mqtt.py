"""
Dispositivo real -> MQTT.

Lee el acelerometro REAL del celular (app phyphox, experimento
"Aceleracion (sin g)", con "Permitir acceso remoto" activado) y publica cada
lectura como evento bici.vibracion en el broker MQTT publico.

Cumple el mismo rol que el ESP32 de Wokwi en 3.8: es la "puerta de salida"
del dispositivo hacia MQTT. No valida rangos ni corrige valores: eso vive en
un solo lugar, consumer_citibike.py.
"""
import json
import math
import os
import time
import urllib.error
import urllib.request
import uuid

import paho.mqtt.client as mqtt

PHYPHOX_URL = os.getenv("PHYPHOX_URL", "http://112.138.0.105:8080").rstrip("/")
MQTT_HOST = os.getenv("MQTT_HOST", "test.mosquitto.org")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "lambda26/citibike/equipo01/vibracion")
BIKE_ID = os.getenv("BIKE_ID", "bici-grimaldo-01")
INTERVAL_MS = int(os.getenv("READ_INTERVAL_MS", "1000"))

BUFFERS = ("accX", "accY", "accZ")


def log(**campos):
    print(json.dumps({"service": "uso-citibike", "component": "lector-phyphox", **campos}))


def http_json(path):
    with urllib.request.urlopen(PHYPHOX_URL + path, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def buffers_disponibles():
    try:
        config = http_json("/config")
        return [b.get("name") for b in config.get("buffers", [])]
    except Exception as ex:  # solo diagnostico
        return [f"no se pudo leer /config: {ex}"]


def iniciar_medicion():
    # Equivale a tocar el boton "play" en la app.
    http_json("/control?cmd=start")


def leer_ultima_lectura():
    """Devuelve (accX, accY, accZ) del ultimo valor medido, o None si aun no hay datos."""
    data = http_json("/get?" + "&".join(BUFFERS))
    buffers = data.get("buffer", {})
    valores = []
    for nombre in BUFFERS:
        if nombre not in buffers:
            raise KeyError(nombre)
        serie = buffers[nombre].get("buffer", [])
        if not serie or serie[-1] is None:
            return None
        valores.append(float(serie[-1]))
    return tuple(valores)


def main():
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"{BIKE_ID}-{uuid.uuid4().hex[:8]}",
    )
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    log(phyphoxUrl=PHYPHOX_URL, mqttHost=MQTT_HOST, mqttTopic=MQTT_TOPIC,
        bikeId=BIKE_ID, intervalMs=INTERVAL_MS, status="connected")

    try:
        iniciar_medicion()
        log(status="measurement_started")
    except Exception as ex:
        log(status="phyphox_unreachable", error=str(ex),
            hint="Revisa PHYPHOX_URL, que el celular este en la misma wifi y la pantalla encendida")

    while True:
        try:
            lectura = leer_ultima_lectura()
        except KeyError as ex:
            log(status="buffer_not_found", buffer=str(ex), disponibles=buffers_disponibles(),
                hint="Abre el experimento 'Aceleracion (sin g)' en phyphox")
            time.sleep(5)
            continue
        except (urllib.error.URLError, TimeoutError, OSError) as ex:
            log(status="phyphox_unreachable", error=str(ex))
            time.sleep(3)
            continue

        if lectura is None:
            log(status="waiting_data", hint="Presiona play en phyphox")
            time.sleep(2)
            continue

        acc_x, acc_y, acc_z = lectura
        evento = {
            "tipoEvento": "bici.vibracion",
            "bikeId": BIKE_ID,
            "accX": round(acc_x, 3),
            "accY": round(acc_y, 3),
            "accZ": round(acc_z, 3),
            "magnitud": round(math.sqrt(acc_x**2 + acc_y**2 + acc_z**2), 3),
            "unidad": "m/s2",
            "origen": "phyphox",
            "timestamp": int(time.time() * 1000),
        }
        info = client.publish(MQTT_TOPIC, json.dumps(evento))
        log(mqttTopic=MQTT_TOPIC, bikeId=BIKE_ID, magnitud=evento["magnitud"],
            timestamp=evento["timestamp"],
            status="published" if info.rc == mqtt.MQTT_ERR_SUCCESS else f"publish_failed:{info.rc}")

        time.sleep(INTERVAL_MS / 1000)


if __name__ == "__main__":
    main()
