# S6 — Contratos de eventos

Contratos de los eventos que circulan en el flujo de eventos empresariales de
`lambda26`, en el formato de las Tablas 4 y 5 de la guía
[S6 - Ingesta de Eventos Empresariales en Tiempo Real](sesiones/S06_Ingesta_Eventos_Empresariales_Kafka.md).
Los tres JSON de ejemplo son mensajes **reales**, leídos con
`kafka-console-consumer.sh` de los topics `orden-eventos` y `pago-eventos`
después de la corrida de verificación del 2026-09-25 — no son plantillas
escritas a mano.

El contrato es la interfaz real entre servicios que no comparten base de datos:
quien construya un tercer consumidor de `orden-eventos` (un servicio de
notificaciones, por ejemplo) necesita saber exactamente qué campos esperar sin
tener que leer el código fuente de `ec-orden-ms`.

## Tabla 1. Contrato del evento `orden.creada`

| Campo | Valor |
|---|---|
| Topic | `orden-eventos` |
| Particiones | 1 |
| Key | `ordenId` (como texto) |
| Productores | `ec-orden-ms`, `ec-eventos-py` |
| Consumidores | `ec-pago-ms`, `ec-eventos-py` |
| Disparador | `POST /ordenes` |
| Campos obligatorios | `tipoEvento`, `ordenId`, `total` |

```json
{
  "tipoEvento": "orden.creada",
  "ordenId": 3,
  "total": 1500.0,
  "estado": "PENDIENTE",
  "origen": "ec-orden-ms",
  "timestamp": 1790364305591
}
```

## Tabla 2. Contrato del evento de pago

| Campo | Valor |
|---|---|
| Topic | `pago-eventos` |
| Particiones | 1 |
| Key | `ordenId` (como texto) |
| Productor | `ec-pago-ms` |
| Consumidores | ninguno todavía (se consumirá desde Spark en S8) |
| Disparador | consumo de un `orden.creada` válido |
| Campos obligatorios | `tipoEvento`, `ordenId`, `monto` |

```json
{
  "tipoEvento": "pago.aprobado",
  "ordenId": 2,
  "monto": 150.0,
  "estado": "APROBADO",
  "origen": "ec-pago-ms",
  "timestamp": 1790364302417
}
```

`tipoEvento` es `pago.rechazado` y `estado` es `RECHAZADO` cuando el monto es
mayor o igual a `1000` (constante `LIMITE_APROBACION` en `ConsumidorPago`) —
mismo esquema, mismo topic, distinto valor:

```json
{
  "tipoEvento": "pago.rechazado",
  "ordenId": 3,
  "monto": 1500.0,
  "estado": "RECHAZADO",
  "origen": "ec-pago-ms",
  "timestamp": 1790364305642
}
```

## Tabla 3. Contrato del evento `orden.cancelada`

Evento propio, agregado como extensión autónoma de la sesión (actividad 4.1 de
la guía).

| Campo | Valor |
|---|---|
| Topic | `orden-eventos` (el mismo de `orden.creada`) |
| Particiones | 1 |
| Key | `ordenId` (como texto) |
| Productor | `ec-orden-ms` |
| Consumidores | `ec-pago-ms` (lo registra, no genera pago) |
| Disparador | `PUT /ordenes/{id}/cancelar` |
| Campos obligatorios | `tipoEvento`, `ordenId` |

```json
{
  "tipoEvento": "orden.cancelada",
  "ordenId": 2,
  "total": 150.0,
  "estado": "CANCELADA",
  "origen": "ec-orden-ms",
  "timestamp": 1790364547680
}
```

**Por qué el mismo topic y no uno nuevo:** `orden.cancelada` describe un cambio
de estado de la *misma* entidad que `orden.creada`, y comparte la key
(`ordenId`). Publicar ambos en `orden-eventos` con la misma key garantiza que
Kafka los entregue **en orden** a un mismo consumidor: ningún consumidor podrá
ver la cancelación de una orden antes que su creación. Si `orden.cancelada`
fuera a un topic aparte, ese orden relativo se perdería, porque Kafka solo
garantiza el orden dentro de una partición. El campo `tipoEvento` es lo que
permite a cada consumidor decidir qué hacer con cada mensaje.

**Por qué `ec-pago-ms` no genera un pago al recibirlo:** una orden cancelada no
tiene pago que procesar. El consumidor lo registra explícitamente con
`status=received-not-processed` en lugar de descartarlo en silencio, para que el
log demuestre que el evento **llegó** y que la decisión de no procesarlo fue
deliberada, no un mensaje perdido.

## Validación del contrato en el consumidor

`ec-pago-ms` defiende el contrato en dos capas distintas, que responden a fallas
distintas:

| Falla | Dónde se detecta | Qué hace el consumidor |
|---|---|---|
| El mensaje no es JSON válido (texto plano) | `ErrorHandlingDeserializer` + `DefaultErrorHandler` (capa Kafka) | Registra `Mensaje descartado ... offset=N`, confirma el offset y sigue con el siguiente mensaje |
| Es JSON válido pero le falta un campo obligatorio (`ordenId` o `total`) | Validación explícita en `ConsumidorPago` (capa aplicación) | Registra `status=invalid motivo="falta un campo obligatorio del contrato"` y descarta el evento |
| `tipoEvento` desconocido | Validación explícita en `ConsumidorPago` | Registra `status=ignored` |

La primera capa es necesaria porque un mensaje que no deserializa **nunca llega**
al método del listener: sin `ErrorHandlingDeserializer`, el contenedor
reintentaría el mismo mensaje indefinidamente y el consumidor quedaría bloqueado
en ese offset. La segunda es necesaria porque Jackson **no** falla ante un campo
ausente: un `orden.creada` sin `ordenId` deserializa perfectamente, con
`ordenId = null`, y sin la validación explícita generaría un pago huérfano en la
tabla `pagos`. En ninguno de los tres casos el consumidor se cae.
