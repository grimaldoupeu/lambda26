package pe.edu.upeu.ec.pago.service;

import pe.edu.upeu.ec.pago.entity.Pago;
import pe.edu.upeu.ec.pago.event.EventoOrden;
import pe.edu.upeu.ec.pago.event.EventoPago;
import pe.edu.upeu.ec.pago.repository.PagoRepositorio;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConsumidorPago {

    private static final String TIPO_EVENTO_ORDEN_CREADA = "orden.creada";
    private static final String TIPO_EVENTO_ORDEN_CANCELADA = "orden.cancelada";
    private static final String TIPO_EVENTO_PAGO_APROBADO = "pago.aprobado";
    private static final String TIPO_EVENTO_PAGO_RECHAZADO = "pago.rechazado";
    private static final String ESTADO_APROBADO = "APROBADO";
    private static final String ESTADO_RECHAZADO = "RECHAZADO";
    private static final double LIMITE_APROBACION = 1000;

    private final PagoRepositorio pagoRepositorio;
    private final ProductorPago productorPago;
    @Value("${spring.application.name}")
    private String applicationName;
    @Value("${app.kafka.topic.ordenes}")
    private String topicOrdenes;
    @Value("${app.kafka.group-id.pagos}")
    private String groupIdPagos;

    @KafkaListener(
            topics = "${app.kafka.topic.ordenes}",
            groupId = "${app.kafka.group-id.pagos}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumirEventoOrden(EventoOrden eventoOrden) {
        if (eventoOrden == null) {
            log.warn(
                    "service=ec-pago-ms component=consumer topic={} groupId={} status=invalid motivo=\"payload nulo, no cumple el contrato\"",
                    topicOrdenes,
                    groupIdPagos
            );
            return;
        }

        if (TIPO_EVENTO_ORDEN_CANCELADA.equals(eventoOrden.getTipoEvento())) {
            log.info(
                    "service=ec-pago-ms component=consumer topic={} groupId={} eventType={} ordenId={} timestamp={} status=received-not-processed motivo=\"orden cancelada, no genera pago\"",
                    topicOrdenes,
                    groupIdPagos,
                    eventoOrden.getTipoEvento(),
                    eventoOrden.getOrdenId(),
                    eventoOrden.getTimestamp()
            );
            return;
        }

        if (!TIPO_EVENTO_ORDEN_CREADA.equals(eventoOrden.getTipoEvento())) {
            log.warn(
                    "service=ec-pago-ms component=consumer topic={} groupId={} eventType={} status=ignored motivo=\"tipoEvento no manejado por este consumer\"",
                    topicOrdenes,
                    groupIdPagos,
                    eventoOrden.getTipoEvento()
            );
            return;
        }

        if (eventoOrden.getOrdenId() == null || eventoOrden.getTotal() == null) {
            log.warn(
                    "service=ec-pago-ms component=consumer topic={} groupId={} eventType={} ordenId={} total={} status=invalid motivo=\"falta un campo obligatorio del contrato\"",
                    topicOrdenes,
                    groupIdPagos,
                    eventoOrden.getTipoEvento(),
                    eventoOrden.getOrdenId(),
                    eventoOrden.getTotal()
            );
            return;
        }

        long processedAt = Instant.now().toEpochMilli();
        Long latencyMs = eventoOrden.getTimestamp() != null ? processedAt - eventoOrden.getTimestamp() : null;

        log.info(
                "service=ec-pago-ms component=consumer topic={} groupId={} eventType={} ordenId={} timestamp={} processedAt={} latencyMs={} status=consumed",
                topicOrdenes,
                groupIdPagos,
                eventoOrden.getTipoEvento(),
                eventoOrden.getOrdenId(),
                eventoOrden.getTimestamp(),
                processedAt,
                latencyMs
        );

        boolean pagoAprobado = eventoOrden.getTotal() != null && eventoOrden.getTotal() < LIMITE_APROBACION;
        String estadoPago = pagoAprobado ? ESTADO_APROBADO : ESTADO_RECHAZADO;
        String tipoEventoPago = pagoAprobado ? TIPO_EVENTO_PAGO_APROBADO : TIPO_EVENTO_PAGO_RECHAZADO;

        Pago pago = Pago.builder()
                .ordenId(eventoOrden.getOrdenId())
                .monto(eventoOrden.getTotal())
                .estado(estadoPago)
                .build();

        pagoRepositorio.save(pago);

        EventoPago eventoPago = EventoPago.builder()
                .tipoEvento(tipoEventoPago)
                .ordenId(eventoOrden.getOrdenId())
                .monto(eventoOrden.getTotal())
                .estado(estadoPago)
                .origen(applicationName)
                .timestamp(Instant.now().toEpochMilli())
                .build();

        productorPago.enviarEventoPago(eventoPago);

        log.info(
                "service=ec-pago-ms component=processor ordenId={} estadoPago={} status=processed",
                eventoOrden.getOrdenId(),
                estadoPago
        );
    }
}
