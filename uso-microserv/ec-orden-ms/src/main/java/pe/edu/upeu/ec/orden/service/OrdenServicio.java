package pe.edu.upeu.ec.orden.service;

import pe.edu.upeu.ec.orden.entity.Orden;
import pe.edu.upeu.ec.orden.event.EventoOrden;
import pe.edu.upeu.ec.orden.repository.OrdenRepositorio;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OrdenServicio {

    private static final String ESTADO_PENDIENTE = "PENDIENTE";
    private static final String ESTADO_CANCELADA = "CANCELADA";
    private static final String TIPO_EVENTO_ORDEN_CREADA = "orden.creada";
    private static final String TIPO_EVENTO_ORDEN_CANCELADA = "orden.cancelada";

    private final OrdenRepositorio ordenRepositorio;
    private final ProductorOrden productorOrden;
    @Value("${spring.application.name}")
    private String applicationName;

    public List<Orden> listarOrdenes() {
        return ordenRepositorio.findAll();
    }

    public Orden crearOrden(Orden orden) {
        orden.setId(null);
        orden.setEstado(ESTADO_PENDIENTE);

        Orden ordenGuardada = ordenRepositorio.save(orden);

        EventoOrden eventoOrden = EventoOrden.builder()
                .tipoEvento(TIPO_EVENTO_ORDEN_CREADA)
                .ordenId(ordenGuardada.getId())
                .total(ordenGuardada.getTotal())
                .estado(ordenGuardada.getEstado())
                .origen(applicationName)
                .timestamp(Instant.now().toEpochMilli())
                .build();

        productorOrden.publicarOrdenCreada(eventoOrden);

        return ordenGuardada;
    }

    public Optional<Orden> cancelarOrden(Long id) {
        return ordenRepositorio.findById(id).map(orden -> {
            if (ESTADO_CANCELADA.equals(orden.getEstado())) {
                return orden;
            }

            orden.setEstado(ESTADO_CANCELADA);
            Orden ordenCancelada = ordenRepositorio.save(orden);

            EventoOrden eventoOrden = EventoOrden.builder()
                    .tipoEvento(TIPO_EVENTO_ORDEN_CANCELADA)
                    .ordenId(ordenCancelada.getId())
                    .total(ordenCancelada.getTotal())
                    .estado(ordenCancelada.getEstado())
                    .origen(applicationName)
                    .timestamp(Instant.now().toEpochMilli())
                    .build();

            productorOrden.publicarOrdenCancelada(eventoOrden);

            return ordenCancelada;
        });
    }
}
