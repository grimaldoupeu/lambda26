package pe.edu.upeu.ec.orden.service;

import pe.edu.upeu.ec.orden.event.EventoOrden;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ProductorOrden {

    private final KafkaTemplate<String, EventoOrden> kafkaTemplate;
    @Value("${app.kafka.topic.ordenes}")
    private String topicOrdenes;

    public void publicarOrdenCreada(EventoOrden eventoOrden) {
        publicar(eventoOrden);
    }

    public void publicarOrdenCancelada(EventoOrden eventoOrden) {
        publicar(eventoOrden);
    }

    private void publicar(EventoOrden eventoOrden) {
        kafkaTemplate.send(topicOrdenes, String.valueOf(eventoOrden.getOrdenId()), eventoOrden)
                .whenComplete((resultado, ex) -> {
                    if (ex != null) {
                        log.error(
                                "service=ec-orden-ms component=producer topic={} eventType={} ordenId={} timestamp={} status=error error=\"{}\"",
                                topicOrdenes,
                                eventoOrden.getTipoEvento(),
                                eventoOrden.getOrdenId(),
                                eventoOrden.getTimestamp(),
                                ex.getMessage()
                        );
                        return;
                    }

                    log.info(
                            "service=ec-orden-ms component=producer topic={} partition={} offset={} eventType={} ordenId={} timestamp={} status=published",
                            resultado.getRecordMetadata().topic(),
                            resultado.getRecordMetadata().partition(),
                            resultado.getRecordMetadata().offset(),
                            eventoOrden.getTipoEvento(),
                            eventoOrden.getOrdenId(),
                            eventoOrden.getTimestamp()
                    );
                });
    }
}
