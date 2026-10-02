#include <WiFi.h>
#include <PubSubClient.h>
#include <DHT.h>
#include <ArduinoJson.h>
#include <time.h>

// --- WiFi simulado de Wokwi: sale a internet real a través del IoT Gateway ---
const char *WIFI_SSID = "Wokwi-GUEST";
const char *WIFI_PASSWORD = "";

// --- Broker MQTT público — sin cuenta, sin túnel, sin tarjeta ---
const char *MQTT_HOST = "test.mosquitto.org";
const int MQTT_PORT = 1883;

// test.mosquitto.org es PÚBLICO: cualquiera en internet puede publicar o
// suscribirse a cualquier topic. "equipo01" evita que tus mensajes se
// mezclen con los de otro equipo del curso — cambia esto por tu propio ID.
const char *MQTT_TOPIC = "lambda26/atmos/equipo01/lecturas";

// --- Identificador de este dispositivo simulado (también debe ser único) ---
const char *SENSOR_ID = "esp32-wokwi-equipo01";

#define DHT_PIN 4
#define DHT_TYPE DHT22

DHT dht(DHT_PIN, DHT_TYPE);

// Wokwi no tiene una pieza BMP280 nativa (los "chip-bmp280" que existen en
// otros proyectos son Custom Chips con archivos propios, no una pieza
// disponible por defecto) — en su lugar, un potenciómetro (SÍ es una pieza
// real de Wokwi, docs.wokwi.com/parts/wokwi-potentiometer) hace de sensor
// de presión: gíralo en vivo durante la simulación para cambiar el valor.
#define PRESSURE_PIN 34

float leerPresion() {
  int lectura = analogRead(PRESSURE_PIN); // 0-4095 (ADC de 12 bits)
  return 995.0 + (lectura / 4095.0) * 30.0; // mapeado a 995-1025 hPa
}

WiFiClient wifiClient;
PubSubClient mqttClient(wifiClient);

unsigned long ultimaLectura = 0;
const unsigned long INTERVALO_MS = 5000;

void conectarWifi() {
  Serial.printf("Conectando a WiFi %s...\n", WIFI_SSID);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println();
  Serial.print("WiFi conectado, IP: ");
  Serial.println(WiFi.localIP());
}

void sincronizarReloj() {
  // Necesario para que "timestamp" sea un epoch real, no segundos desde el
  // arranque — el bridge y el consumer calculan latencyMs a partir de esto.
  configTime(0, 0, "pool.ntp.org", "time.nist.gov");
  Serial.print("Sincronizando reloj por NTP");
  time_t ahora = time(nullptr);
  while (ahora < 1700000000) { // fecha muy antigua = todavia no sincronizo
    delay(500);
    Serial.print(".");
    ahora = time(nullptr);
  }
  Serial.println(" listo");
}

void conectarMQTT() {
  while (!mqttClient.connected()) {
    Serial.printf("Conectando a MQTT %s:%d...\n", MQTT_HOST, MQTT_PORT);
    if (mqttClient.connect(SENSOR_ID)) {
      Serial.println("MQTT conectado");
    } else {
      Serial.printf("Fallo MQTT, rc=%d. Reintentando en 2s\n", mqttClient.state());
      delay(2000);
    }
  }
}

void setup() {
  Serial.begin(115200);
  dht.begin();

  conectarWifi();
  sincronizarReloj();
  mqttClient.setServer(MQTT_HOST, MQTT_PORT);
  conectarMQTT();
}

void loop() {
  if (!mqttClient.connected()) {
    conectarMQTT();
  }
  mqttClient.loop();

  unsigned long ahora = millis();
  if (ahora - ultimaLectura >= INTERVALO_MS) {
    ultimaLectura = ahora;

    float temperatura = dht.readTemperature();
    float humedad = dht.readHumidity();
    float presion = leerPresion();

    if (isnan(temperatura) || isnan(humedad)) {
      Serial.println("Lectura invalida del DHT22, se omite esta ronda");
      return;
    }

    StaticJsonDocument<256> doc;
    doc["tipoEvento"] = "sensor.lectura";
    doc["sensorId"] = SENSOR_ID;
    doc["temperatura"] = temperatura;
    doc["humedad"] = humedad;
    doc["presion"] = presion;
    doc["origen"] = "wokwi";
    doc["timestamp"] = (unsigned long long)time(nullptr) * 1000ULL;

    char payload[256];
    serializeJson(doc, payload);

    if (mqttClient.publish(MQTT_TOPIC, payload)) {
      Serial.print("Publicado: ");
      Serial.println(payload);
    } else {
      Serial.println("Fallo al publicar en MQTT");
    }
  }
}
