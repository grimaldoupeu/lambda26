# uso-citibike — S07 con sensor real (celular + phyphox)

Copia esta carpeta junto a `uso-atmos` dentro de tu repo `lambda26`. Todos los comandos son para **PowerShell**, desde la raíz de `lambda26`.

## 0. Antes de empezar
1. En `compose.yml` pon en `PHYPHOX_URL` la dirección que funcione (prueba ambas en el navegador de la laptop; la que muestre la página de phyphox es la correcta) y cambia `equipo01` por tu equipo.
2. En el celular: phyphox abierto en **Aceleración (sin g)**, acceso remoto activado, pantalla encendida.

## 1. Kafka y topic
```powershell
docker compose -f kafka/compose.yml ps
docker compose -f kafka/compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic citibike-eventos --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
```

## 2. Levantar el módulo
```powershell
docker compose -f uso-citibike/compose.yml up -d --build
```

## 3. Ejecutar (una terminal por script, en este orden)
```powershell
# Terminal 1 - consumer
docker compose -f uso-citibike/compose.yml exec uso-citibike python /app/consumer_citibike.py
# Terminal 2 - puente MQTT -> Kafka
docker compose -f uso-citibike/compose.yml exec uso-citibike python /app/bridge_mqtt_kafka.py
# Terminal 3 - lector del celular -> MQTT
docker compose -f uso-citibike/compose.yml exec uso-citibike python /app/lector_phyphox_mqtt.py
# Terminal 4 (opcional) - feed real de estaciones Citi Bike
docker compose -f uso-citibike/compose.yml exec uso-citibike python /app/producer_gbfs.py
```

## 4. Evidencias a capturar
| # | Qué | Cómo |
|---|---|---|
| 1 | Sensor real | Video/foto del celular con phyphox midiendo + la terminal 3 publicando |
| 2 | Kafka UI | `http://localhost:48085` → topic `citibike-eventos` → pestaña de particiones |
| 3 | Punta a punta | Terminales 2 y 3 (`forwarded`, `published`) y 1 (`consumed`) |
| 4 | Contrato | `CONTRATO.md` |
| 5 | Alerta real | **Sacude el celular fuerte** → en la terminal 1 aparece `status: "alerta"` |
| extra | `invalid` | Publica texto plano al topic desde Kafka UI → `status: "invalid"` |

## Problemas comunes
- `phyphox_unreachable`: IP equivocada, otra red wifi, o la pantalla del celular se apagó (phyphox deja de responder). Desactiva el bloqueo automático mientras pruebas.
- `buffer_not_found`: no estás en el experimento "Aceleración (sin g)"; el log muestra los buffers disponibles.
- `waiting_data`: presiona ▶ en phyphox.
