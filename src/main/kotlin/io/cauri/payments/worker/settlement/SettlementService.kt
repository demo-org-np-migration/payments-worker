package io.cauri.payments.worker.settlement

import io.cauri.payments.worker.events.PaymentCompletedEvent
import org.slf4j.LoggerFactory
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.util.UUID

/** Idempotencia del consumer: `payment_id` único en `settlements` decide si un `payments.completed`
 *  ya fue procesado. Si ya existe, no se inserta de nuevo y tampoco se vuelve a encolar la
 *  notificación (eso lo decide el caller mirando el resultado de [registerIfNew]). */
@Service
class SettlementService(
    private val settlementRepository: SettlementRepository,
) {
    private val log = LoggerFactory.getLogger(SettlementService::class.java)

    @Transactional
    fun registerIfNew(event: PaymentCompletedEvent, batchRef: String?): Boolean {
        if (settlementRepository.existsByPaymentId(event.paymentId)) {
            log.info("settlement already exists for payment_id={}, ignoring duplicate", event.paymentId)
            return false
        }

        val settlement = Settlement(
            id = UUID.randomUUID(),
            paymentId = event.paymentId,
            settledAt = Instant.now(),
            batchRef = batchRef,
        )

        return try {
            settlementRepository.save(settlement)
            log.info("settlement created for payment_id={}", event.paymentId)
            true
        } catch (ex: DataIntegrityViolationException) {
            // Carrera entre dos entregas del mismo mensaje (p.ej. rebalance de partición):
            // el unique constraint de payment_id es la última línea de defensa.
            log.info("settlement insert raced for payment_id={}, treating as duplicate", event.paymentId)
            false
        }
    }
}
