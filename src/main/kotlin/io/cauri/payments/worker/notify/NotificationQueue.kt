package io.cauri.payments.worker.notify

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import io.cauri.payments.worker.events.PaymentCompletedEvent
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest
import software.amazon.awssdk.services.sqs.model.SendMessageRequest

/** Mensaje de `<SQS_QUEUE_PREFIX>notifications` (las convenciones internas de API). `account_id` es el
 *  `from_account` del pago: la cola avisa a quien pagó, no a quien cobró. */
data class NotificationMessage(
    val type: String = "payment.completed",
    @JsonProperty("account_id") val accountId: String,
    @JsonProperty("payment_id") val paymentId: String,
    val amount: String,
    val currency: String,
)

/** Encola en `<SQS_QUEUE_PREFIX>notifications`. La URL de la cola se resuelve una sola vez con
 *  `GetQueueUrl` (las convenciones internas de API exige resolverla, no armarla a mano) y se cachea para el resto de
 *  la vida del proceso. Si SQS falla, la excepción del SDK se propaga tal cual: el `@KafkaListener`
 *  que llama a [enqueue] no debe atrapar esto, así el offset de Kafka no avanza y el mensaje se
 *  reintenta (ver PaymentCompletedListener). */
@Component
class NotificationQueue(
    private val sqsClient: SqsClient,
    @Value("\${aws.sqs.queue-name}") private val queueName: String,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(NotificationQueue::class.java)

    private val queueUrl: String by lazy {
        sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).queueUrl()
    }

    fun enqueue(event: PaymentCompletedEvent) {
        val message = NotificationMessage(
            accountId = event.fromAccount.toString(),
            paymentId = event.paymentId.toString(),
            amount = event.amount,
            currency = event.currency,
        )
        val body = objectMapper.writeValueAsString(message)

        try {
            sqsClient.sendMessage(
                SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(body)
                    .build(),
            )
            log.info("enqueued notification for payment_id={} queue={}", event.paymentId, queueName)
        } catch (ex: Exception) {
            log.error("failed to enqueue notification for payment_id={} queue={}: {}", event.paymentId, queueName, ex.message)
            throw ex
        }
    }
}
