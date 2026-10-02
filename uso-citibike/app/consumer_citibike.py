"""
Consumer del Proyecto Sello (mismo patron que consumer_sensores.py).

Dos validaciones SEPARADAS, en este orden:
  1. Esquema  -> JSON valido, tipoEvento conocido, campos requeridos con su tipo.
                 Si falla: status = "invalid".
  2. Rango fisico -> solo si el esquema es valido. Valor con forma correcta
                 pero fisicamente implausible. Si falla: status = "alerta".
  Si pasa ambas: status = "consumed".
"""
import json
import os
import time

NUM = "number"
INT = "integer"
STR = "string"

# Contrato por tipo de evento (ver CONTRATO.md)
ESQUEMAS = {
    "bici.vibracion": {
        "bikeId": STR, "accX": NUM, "accY": NUM, "accZ": NUM,
        "magnitud": NUM, "unidad": STR, "origen": STR, "timestamp": INT,
    },
    "estacion.estado": {
        "stationId": STR, "bicisDisponibles": INT, "docksDisponibles": INT,
        "origen": STR, "timestamp": INT,
    },
}

# Rango fisico plausible (ver justificacion en CONTRATO.md)
RANGO_FISICO = {
    "bici.vibracion": {
        "accX": (-25.0, 25.0),
        "accY": (-25.0, 25.0),
        "accZ": (-25.0, 25.0),
        "magnitud": (0.0, 25.0),
    },
    "estacion.estado": {
        "bicisDisponibles": (0, 150),
        "docksDisponibles": (0, 150),
    },
}


def es_tipo(valor, tipo):
    if isinstance(valor, bool):          # True/False no cuentan como numero
        return False
    if tipo == NUM:
        return isinstance(valor, (int, float))
    if tipo == INT:
        return isinstance(valor, int)
    return isinstance(valor, str) and valor != ""


def validar_esquema(decoded):
    """Devuelve (es_valido, motivo)."""
    if not decoded["isJson"]:
        return False, "no es JSON"
    evento = decoded["payload"]
    if not isinstance(evento, dict):
        return False, "el JSON no es un objeto"
    tipo = evento.get("tipoEvento")
    if tipo not in ESQUEMAS:
        return False, f"tipoEvento desconocido: {tipo}"
    for campo, tipo_campo in ESQUEMAS[tipo].items():
        if campo not in evento or evento[campo] is None:
            return False, f"falta el campo {campo}"
        if not es_tipo(evento[campo], tipo_campo):
            return False, f"{campo} deberia ser {tipo_campo}"
    return True, None


def fuera_de_rango(evento):
    """Lista de variables fuera de su rango fisico (vacia si todo esta bien)."""
    fuera = []
    for variable, (minimo, maximo) in RANGO_FISICO[evento["tipoEvento"]].items():
        valor = evento[variable]
        if not (minimo <= valor <= maximo):
            fuera.append(variable)
    return fuera


def deserialize_message(value):
    text = value.decode("utf-8", errors="replace")
    try:
        return {"payload": json.loads(text), "raw": text, "isJson": True, "decodeError": None}
    except json.JSONDecodeError as ex:
        return {"payload": None, "raw": text, "isJson": False, "decodeError": str(ex)}


def procesar(msg_topic, partition, offset, key, decoded, group_id):
    es_valido, motivo = validar_esquema(decoded)
    evento = decoded["payload"] if es_valido else (
        decoded["payload"] if isinstance(decoded["payload"], dict) else {})
    variables_fuera = fuera_de_rango(evento) if es_valido else []
    alerta = bool(variables_fuera)

    timestamp = evento.get("timestamp") if isinstance(evento.get("timestamp"), int) else None
    processed_at = int(time.time() * 1000)

    log = {
        "service": "uso-citibike",
        "component": "consumer",
        "topic": msg_topic,
        "partition": partition,
        "offset": offset,
        "key": key,
        "groupId": group_id,
        "eventType": evento.get("tipoEvento"),
        "bikeId": evento.get("bikeId"),
        "stationId": evento.get("stationId"),
        "magnitud": evento.get("magnitud"),
        "bicisDisponibles": evento.get("bicisDisponibles"),
        "timestamp": timestamp,
        "isValid": es_valido,
        "motivoInvalido": motivo,
        "fueraDeRango": alerta,
        "variablesFueraDeRango": variables_fuera,
        "processedAt": processed_at,
        "latencyMs": processed_at - timestamp if timestamp is not None else None,
        "rawPayload": decoded["raw"] if not es_valido else None,
        "decodeError": decoded["decodeError"],
        "status": "alerta" if alerta else ("consumed" if es_valido else "invalid"),
    }
    # Quita campos vacios para que el log sea legible en las capturas
    return {k: v for k, v in log.items() if v is not None and v != []}


def main():
    from kafka import KafkaConsumer

    topic = os.getenv("KAFKA_TOPIC_CITIBIKE", "citibike-eventos")
    group_id = os.getenv("KAFKA_GROUP_ID", "uso-citibike-group")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=deserialize_message,
        key_deserializer=lambda k: k.decode("utf-8") if k is not None else None,
    )
    print(json.dumps({"service": "uso-citibike", "component": "consumer", "topic": topic,
                      "groupId": group_id, "bootstrapServers": bootstrap, "status": "listening"}))

    for msg in consumer:
        print(json.dumps(procesar(msg.topic, msg.partition, msg.offset, msg.key, msg.value, group_id)))


if __name__ == "__main__":
    main()
