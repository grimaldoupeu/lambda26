# Firmware Wokwi (ESP32 + DHT22 + potenciómetro)

Simulación de hardware real (no un script Python fingiendo ser un sensor):
un ESP32 corriendo firmware Arduino de verdad, con un DHT22 (temperatura y
humedad) conectado en un circuito virtual, publicando por MQTT hacia el
puente de `uso-atmos` (`bridge_mqtt_kafka.py`). La presión se simula con un
**potenciómetro** (girable en vivo durante la simulación) — Wokwi no tiene
ninguna pieza de sensor de presión barométrica nativa; los `chip-bmp280` que
aparecen en proyectos de otras personas son *Custom Chips* con archivos
propios, no piezas disponibles por defecto (verificado probando varios
nombres reales, todos fallaron con "Missing chip Breakout").

Verificado funcionando de punta a punta (WiFi, NTP, MQTT, DHT22 y
potenciómetro) contra un proyecto real en Wokwi.

## Requisitos

- Cuenta gratuita en [wokwi.com](https://wokwi.com).
- El bridge de `uso-atmos` corriendo (ver el README de la carpeta padre) —
  no hace falta Mosquitto propio ni túnel: tanto el ESP32 simulado como el
  bridge se conectan al broker público `test.mosquitto.org`.

## Uso

1. Entra directo a [wokwi.com/projects/new/esp32](https://wokwi.com/projects/new/esp32) — te lleva a un proyecto en blanco con el ESP32 genérico. Si en cambio entras a `wokwi.com/esp32` y navegas a mano, cuidado: "Featured projects" (arriba) son proyectos de ejemplo de otras personas, no plantillas en blanco — la plantilla correcta está en "Starter Templates", tarjeta **"ESP32"** (no S2/S3/C3/C6/H2, es otro chip con otros pines por defecto).
2. Reemplaza el `sketch.ino` generado por el de esta carpeta.
3. Reemplaza el `diagram.json` generado por el de esta carpeta (agrega el
   DHT22 en el pin `4` y el potenciómetro en el pin `34` — sin el prefijo
   `D` que usan otras placas de Wokwi; la plantilla `board-esp32-devkit-c-v4`
   nombra los GPIO con el número pelado).
4. Click en la pestaña **"Library Manager"** (al lado de `diagram.json`) y
   agrega, una por una, las 3 librerías de `libraries.txt`: búscalas por
   nombre y agrega la que coincida exactamente (`PubSubClient` es la de
   **Nick O'Leary**). No basta con que el archivo `libraries.txt` exista en
   el proyecto — cada librería se agrega manualmente desde esta pestaña, o
   la compilación falla con `fatal error: ....h: No such file or directory`
   aunque el código esté bien. Verifica en "Installed Libraries" que las 3
   aparezcan antes de compilar.
5. En `sketch.ino`, cambia `equipo01` (en `MQTT_TOPIC` y en `SENSOR_ID`) por
   tu propio identificador de equipo — el mismo que usaste en el bridge
   (README de la carpeta padre). `test.mosquitto.org` es un broker público:
   ese identificador evita que tus mensajes se mezclen con los de otro
   equipo del curso.
6. Click en **Start Simulation**.

## Qué esperar

En el Monitor Serial de Wokwi verás la conexión a WiFi (`Wokwi-GUEST`, sin
contraseña — la red simulada de Wokwi con salida real a internet), la
sincronización de reloj por NTP, la conexión a `test.mosquitto.org`, y una
línea `Publicado: {...}` cada 5 segundos con la lectura real de los sensores
simulados. Del lado de `uso-atmos`, el log del bridge debe mostrar
`status: "forwarded"` por cada lectura, y el consumer (`consumer_sensores.py`,
ya construido en 3.4) debe procesarla exactamente igual que las del productor
Python — mismo esquema, mismas validaciones, sin ningún cambio de código.

Tanto el DHT22 como el potenciómetro se pueden ajustar en vivo mientras la
simulación corre: click sobre el DHT22 abre un panel "Editing DHT22" con
sliders de **Temperature** y **Humidity**; arrastrar el potenciómetro cambia
la lectura de presión simulada (`leerPresion()` en `sketch.ino`). Sirve para
probar casos de borde — por ejemplo, valores fuera de rango físico y ver
cómo los rechaza `consumer_sensores.py` — sin tocar una línea de código.

**Error frecuente**: las lecturas dejan de llegar al bridge/consumer sin
ningún error visible en el Monitor Serial. La CPU del ESP32 se emula en tu
navegador (WebAssembly, del lado del cliente) — Wokwi **pausa la simulación
entera** (WiFi y MQTT incluidos) en cuanto la pestaña deja de estar activa:
minimizar la ventana, cambiar de pestaña o bloquear la pantalla la congela.
Es una limitación conocida y sin solución del lado de Wokwi, no un bug de
este firmware: mantén la pestaña visible y en primer plano mientras dure la
prueba.

**Error frecuente**: una pieza aparece como un recuadro verde que dice
"Missing chip Breakout" en vez de dibujarse en el circuito — esa pieza no
existe de verdad en el catálogo de Wokwi (nos pasó con varios nombres
inventados de BMP280 antes de reemplazarlo por el potenciómetro). Usa solo
las piezas confirmadas de esta guía.

**Si no ves texto en el Monitor Serial** aunque la simulación esté
corriendo: revisa la esquina inferior derecha del panel de Simulation — a
veces ese panel queda colapsado detrás de un pequeño ícono de flecha (`⌄`).
También puede estar tapado por el panel "Editing DHT22" (el mismo de arriba)
si lo dejaste abierto — ciérralo con la `X` de su esquina superior derecha.

## Alternativa: hardware real (ESP32 + DHT22 + potenciómetro físicos)

Si tienes el hardware real (no simulado), el mismo `sketch.ino` sirve casi
sin cambios — la única diferencia real es que ahora compilas con el Arduino
IDE y subes el firmware por USB, en vez de correrlo en el navegador de
Wokwi. El resto del pipeline (broker público, el bridge, Kafka) no cambia en
absoluto. Si en cambio quieres usar un **BMP280 real comprado** (a diferencia
de Wokwi, sí existe como módulo físico barato) en vez del potenciómetro,
puedes reemplazar `leerPresion()` por la librería `Adafruit_BMP280` en modo
I2C (`Wire.begin()` + `bmp.begin(0x76)`) — queda fuera del alcance de esta
guía, pero es una sustitución directa.

### 1. Cableado físico

Mismo mapeo de pines que `diagram.json`:

| Sensor | Pin del sensor | Pin del ESP32 |
|---|---|---|
| DHT22 | VCC | 3V3 |
| DHT22 | GND | GND |
| DHT22 | DATA (a veces marcado `OUT` o `SDA`) | GPIO4 |
| Potenciómetro | VCC | 3V3 |
| Potenciómetro | GND | GND |
| Potenciómetro | SIG (cursor central) | GPIO34 |

Si tu DHT22 es el sensor "pelado" (sin placa adaptadora), necesita una
resistencia *pull-up* de 10kΩ entre `DATA` y `VCC` — la mayoría de los
módulos ya la traen integrada, revisa el tuyo. GPIO34 es una entrada **solo
lectura** (input-only) del ESP32 — no intentes usarla como salida en otro
cableado.

### 2. Arduino IDE

1. Instala el [Arduino IDE](https://www.arduino.cc/en/software) (2.x).
2. Agrega soporte para ESP32: `File > Preferences > Additional Boards
   Manager URLs` y pega
   `https://raw.githubusercontent.com/espressif/arduino-esp32/gh-pages/package_esp32_index.json`.
   Luego `Tools > Board > Boards Manager`, busca `esp32` e instala el
   paquete de Espressif Systems.
3. Selecciona tu placa en `Tools > Board > ESP32 Arduino` (usualmente
   "ESP32 Dev Module") y el puerto correcto en `Tools > Port`.
4. Instala desde `Tools > Manage Libraries` las mismas librerías de
   `libraries.txt`: `PubSubClient`, `DHT sensor library`, `ArduinoJson`.

### 3. Adaptar `sketch.ino`

Copia el contenido de `sketch.ino` a un nuevo sketch y cambia solo esto:

- `WIFI_SSID` / `WIFI_PASSWORD`: tu red WiFi real (ya no `"Wokwi-GUEST"`).
  El ESP32 real solo soporta redes de 2.4GHz, no 5GHz.
- `MQTT_HOST` / `MQTT_PORT`: **déjalos igual** (`test.mosquitto.org:1883`) —
  no dependen de si el dispositivo es simulado o real.

No subas a git un `sketch.ino` con tu contraseña de WiFi real dentro —
mantenla solo en tu copia local.

### 4. Compilar, subir y verificar

Conecta el ESP32 por USB, click en **Upload**, y abre el **Monitor Serie**
(115200 baudios). Deberías ver la misma secuencia que en Wokwi: conexión
WiFi, sincronización NTP, conexión MQTT, y `Publicado: {...}` cada 5
segundos — pero ahora con lecturas reales de tu sensor físico, no
simuladas. La verificación de punta a punta (bridge, consumer) es idéntica
a la de la sección "Qué esperar" de arriba.
