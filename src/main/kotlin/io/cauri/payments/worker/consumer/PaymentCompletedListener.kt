package io.cauri.payments.worker.consumer

import io.cauri.payments.worker.events.PaymentCompletedEvent
import io.cauri.payments.worker.notify.NotificationQueue
import io.cauri.payments.worker.settlement.SettlementService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

/** Consume `<KAFKA_TOPIC_PREFIX>payments.completed`, group `payments-worker`. Por cada evento:
 *  1) registra el settlement (ignora duplicados por `payment_id`), y
 *  2) si el settlement era nuevo, encola la notificación al cliente en SQS.
 *  Un evento repetido no vuelve a encolar: si ya existía el settlement, no llamamos a
 *  [NotificationQueue.enqueue] de nuevo. */
@Component
class PaymentCompletedListener(
    private val settlementService: SettlementService,
    private val notificationQueue: NotificationQueue,
) {
    private val log = LoggerFactory.getLogger(PaymentCompletedListener::class.java)

    @KafkaListener(topics = ["\${kafka.topic-prefix}payments.completed"], groupId = "payments-worker")
    fun onMessage(record: ConsumerRecord<String, PaymentCompletedEvent>) {
        val event = record.value()
        log.info(
            "received payments.completed payment_id={} partition={} offset={}",
            event.paymentId, record.partition(), record.offset(),
        )

        val batchRef = "kafka:partition-${record.partition()}:offset-${record.offset()}"
        val isNewSettlement = settlementService.registerIfNew(event, batchRef)

        if (!isNewSettlement) {
            log.info("payment_id={} already settled, skipping notification", event.paymentId)
            return
        }

        notificationQueue.enqueue(event)
    }
}
