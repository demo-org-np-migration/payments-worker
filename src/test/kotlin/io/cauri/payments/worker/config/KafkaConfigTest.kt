package io.cauri.payments.worker.config

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID

/** payments-api publica el evento como JSON plano (sin type headers). Este test deserializa un
 *  payload real de las convenciones internas de API con el mismo ObjectMapper que usa el consumer factory, para
 *  que un cambio que rompa la resolución de nombres de constructor de Kotlin (como pasó una vez:
 *  faltaba `registerKotlinModule()`) truene en `./gradlew test`, no en un cluster real. */
class KafkaConfigTest {

    private val objectMapper = KafkaConfig("localhost:9092").kafkaObjectMapper()

    @Test
    fun `deserializes a real payments-api payload in snake_case`() {
        val paymentId = UUID.randomUUID()
        val fromAccount = UUID.randomUUID()
        val toAccount = UUID.randomUUID()
        val json = """
            {
              "event": "payments.completed",
              "version": 1,
              "payment_id": "$paymentId",
              "from_account": "$fromAccount",
              "to_account": "$toAccount",
              "merchant_id": null,
              "amount": "1250.50",
              "currency": "ARS",
              "occurred_at": "2026-09-09T12:00:00Z"
            }
        """.trimIndent()

        val event = objectMapper.readValue(json, io.cauri.payments.worker.events.PaymentCompletedEvent::class.java)

        assertEquals(paymentId, event.paymentId)
        assertEquals(fromAccount, event.fromAccount)
        assertEquals(toAccount, event.toAccount)
        assertEquals(null, event.merchantId)
        assertEquals("1250.50", event.amount)
        assertEquals("ARS", event.currency)
        assertEquals(Instant.parse("2026-09-09T12:00:00Z"), event.occurredAt)
    }
}
