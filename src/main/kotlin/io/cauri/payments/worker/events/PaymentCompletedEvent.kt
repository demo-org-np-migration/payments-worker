package io.cauri.payments.worker.events

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Instant
import java.util.UUID

/** Payload de `<KAFKA_TOPIC_PREFIX>payments.completed` (las convenciones internas de API), tal como lo publica
 *  payments-api. Se deserializa en snake_case (ver KafkaConfig.kafkaObjectMapper). */
data class PaymentCompletedEvent(
    val event: String = "payments.completed",
    val version: Int = 1,
    @JsonProperty("payment_id") val paymentId: UUID,
    @JsonProperty("from_account") val fromAccount: UUID,
    @JsonProperty("to_account") val toAccount: UUID,
    @JsonProperty("merchant_id") val merchantId: UUID?,
    val amount: String,
    val currency: String,
    @JsonProperty("occurred_at") val occurredAt: Instant,
)
