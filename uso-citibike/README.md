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

## 3. Ejecutar (los cuatro scripts en segundo plano, en este orden)

Cada script corre dentro del contenedor con `exec -d` y escribe su propia
bitacora en `app/logs/`. Asi no hace falta una terminal por script y queda
evidencia en archivo para las capturas.

```powershell
# La carpeta de logs vive en el host y se ve dentro del contenedor (./app:/app)
New-Item -ItemType Directory -Force uso-citibike\app\logs

# 1. consumer (valida esquema y rango)
docker compose -f uso-citibike/compose.yml exec -d uso-citibike sh -c "python -u /app/consumer_citibike.py > /app/logs/consumer.log 2>&1"
# 2. puente MQTT -> Kafka
docker compose -f uso-citibike/compose.yml exec -d uso-citibike sh -c "python -u /app/bridge_mqtt_kafka.py > /app/logs/bridge.log 2>&1"
# 3. lector del celular -> MQTT
docker compose -f uso-citibike/compose.yml exec -d uso-citibike sh -c "python -u /app/lector_phyphox_mqtt.py > /app/logs/lector.log 2>&1"
# 4. feed real de estaciones Citi Bike -> Kafka
docker compose -f uso-citibike/compose.yml exec -d uso-citibike sh -c "python -u /app/producer_gbfs.py > /app/logs/gbfs.log 2>&1"
```

El `-u` de Python es obligatorio: sin el, la salida se queda en el buffer y el
log aparece vacio aunque el script este corriendo bien.

Comprobar que los cuatro arrancaron (las ultimas lineas de cada bitacora):

```powershell
foreach ($f in 'consumer','bridge','lector','gbfs') {
  "===== $f.log ====="
  docker compose -f uso-citibike/compose.yml exec uso-citibike tail -n 5 /app/logs/$f.log
}
```

Se esperan: `published` en `lector`, `forwarded` en `bridge`, `consumed` en
`consumer` y `published` en `gbfs`.

### Ver los logs en vivo

`Get-Content -Wait` es el equivalente de `tail -f` en PowerShell: deja la
ventana abierta y va mostrando cada linea nueva a medida que llega.

```powershell
Get-Content uso-citibike\app\logs\lector.log   -Tail 15 -Wait
Get-Content uso-citibike\app\logs\bridge.log   -Tail 15 -Wait
Get-Content uso-citibike\app\logs\consumer.log -Tail 15 -Wait
Get-Content uso-citibike\app\logs\gbfs.log     -Tail 15 -Wait
```

Una bitacora por ventana. Para las capturas conviene abrir cada una en su
propia ventana, ya ubicada y con titulo propio:

```powershell
Start-Process powershell -ArgumentList '-NoExit','-Command',"Set-Location D:\bigdata\lambda26; `$Host.UI.RawUI.WindowTitle='S07 - LECTOR (celular -> MQTT)'; Get-Content uso-citibike\app\logs\lector.log -Tail 15 -Wait"
```

Para filtrar solo los casos interesantes (es una foto, no sigue en vivo):

```powershell
Select-String '"alerta"'  uso-citibike\app\logs\consumer.log
Select-String '"invalid"' uso-citibike\app\logs\consumer.log
```

Conteo de cada resultado:

```powershell
foreach ($s in 'consumed','alerta','invalid') {
  "$s = " + (Select-String ('"status": "' + $s + '"') uso-citibike\app\logs\consumer.log).Count
}
```

Los logs **no se versionan** (`.gitignore` excluye `uso-citibike/app/logs/`):
son salida de ejecucion y se regeneran en cada corrida.

## 4. Evidencias a capturar
| # | Qué | Cómo |
|---|---|---|
| 1 | Sensor real | Video/foto del celular con phyphox midiendo + la ventana de `lector.log` publicando |
| 2 | Kafka UI | `http://localhost:48085` → topic `citibike-eventos` → pestaña de particiones |
| 3 | Punta a punta | `lector.log` (`published`) -> `bridge.log` (`forwarded`) -> `consumer.log` (`consumed`) |
| 4 | Contrato | `CONTRATO.md` |
| 5 | Alerta real | **Sacude el celular fuerte** → en `consumer.log` aparece `status: "alerta"` |
| extra | `invalid` | Publica texto plano al topic MQTT (o desde Kafka UI) → `status: "invalid"` |

## Problemas comunes
- `phyphox_unreachable`: IP equivocada, otra red wifi, o la pantalla del celular se apagó (phyphox deja de responder). Desactiva el bloqueo automático mientras pruebas.
- `buffer_not_found`: no estás en el experimento "Aceleración (sin g)"; el log muestra los buffers disponibles.
- `waiting_data`: presiona ▶ en phyphox.
