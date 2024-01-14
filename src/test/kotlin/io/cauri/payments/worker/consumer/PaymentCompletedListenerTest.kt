package io.cauri.payments.worker.consumer

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.cauri.payments.worker.events.PaymentCompletedEvent
import io.cauri.payments.worker.notify.NotificationQueue
import io.cauri.payments.worker.settlement.Settlement
import io.cauri.payments.worker.settlement.SettlementRepository
import io.cauri.payments.worker.settlement.SettlementService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse
import software.amazon.awssdk.services.sqs.model.SendMessageRequest
import software.amazon.awssdk.services.sqs.model.SendMessageResponse
import java.time.Instant
import java.util.UUID

/** Cubre el contrato mínimo del spec: un evento crea un settlement y encola la notificación; el
 *  mismo evento dos veces no duplica el settlement ni vuelve a encolar. Usa el repositorio real
 *  contra H2 en modo PostgreSQL (`@DataJpaTest`, ver src/test/resources/application.yml) y un
 *  `SqsClient` mockeado con MockK, tal como pide el spec de payments-worker. */
@DataJpaTest
class PaymentCompletedListenerTest {

    @Autowired
    private lateinit var settlementRepository: SettlementRepository

    private lateinit var sqsClient: SqsClient
    private lateinit var listener: PaymentCompletedListener

    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(JavaTimeModule())
        .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    @BeforeEach
    fun setUp() {
        sqsClient = mockk()
        every { sqsClient.getQueueUrl(any<GetQueueUrlRequest>()) } returns
            GetQueueUrlResponse.builder().queueUrl("http://localstack:4566/000000000000/staging-notifications").build()
        every { sqsClient.sendMessage(any<SendMessageRequest>()) } returns
            SendMessageResponse.builder().messageId("msg-1").build()

        val notificationQueue = NotificationQueue(sqsClient, "staging-notifications", objectMapper)
        val settlementService = SettlementService(settlementRepository)
        listener = PaymentCompletedListener(settlementService, notificationQueue)
    }

    private fun sampleEvent(paymentId: UUID = UUID.randomUUID()) = PaymentCompletedEvent(
        paymentId = paymentId,
        fromAccount = UUID.randomUUID(),
        toAccount = UUID.randomUUID(),
        merchantId = null,
        amount = "1250.50",
        currency = "ARS",
        occurredAt = Instant.now(),
    )

    private fun recordOf(event: PaymentCompletedEvent, offset: Long = 0L) =
        ConsumerRecord("staging.payments.completed", 0, offset, event.paymentId.toString(), event)

    @Test
    fun `an event creates a settlement and enqueues the notification`() {
        val event = sampleEvent()

        listener.onMessage(recordOf(event))

        assertTrue(settlementRepository.existsByPaymentId(event.paymentId))
        verify(exactly = 1) { sqsClient.sendMessage(any<SendMessageRequest>()) }
    }

    @Test
    fun `the same event twice does not duplicate the settlement nor re-enqueue`() {
        val event = sampleEvent()

        listener.onMessage(recordOf(event, offset = 0L))
        listener.onMessage(recordOf(event, offset = 1L))

        val settlements: List<Settlement> = settlementRepository.findAll()
        assertEquals(1, settlements.count { it.paymentId == event.paymentId })
        verify(exactly = 1) { sqsClient.sendMessage(any<SendMessageRequest>()) }
    }

    @Test
    fun `two different events each create their own settlement and each enqueue once`() {
        val first = sampleEvent()
        val second = sampleEvent()

        listener.onMessage(recordOf(first))
        listener.onMessage(recordOf(second))

        assertTrue(settlementRepository.existsByPaymentId(first.paymentId))
        assertTrue(settlementRepository.existsByPaymentId(second.paymentId))
        verify(exactly = 2) { sqsClient.sendMessage(any<SendMessageRequest>()) }
    }
}
