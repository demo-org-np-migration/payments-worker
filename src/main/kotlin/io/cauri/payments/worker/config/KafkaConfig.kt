package io.cauri.payments.worker.config

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.cauri.payments.worker.events.PaymentCompletedEvent
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.util.backoff.FixedBackOff

/** Consumer de `<KAFKA_TOPIC_PREFIX>payments.completed`, group `payments-worker` (las convenciones internas de API,
 *  spec payments-worker). payments-api publica el evento como JSON plano en camelCase-a-snake_case
 *  vía `@JsonProperty` (sin type headers, porque serializa con StringSerializer); acá lo leemos con
 *  un ObjectMapper en `SNAKE_CASE` que ignora campos desconocidos. */
@Configuration
class KafkaConfig(
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
) {

    @Bean
    fun kafkaObjectMapper(): ObjectMapper =
        ObjectMapper()
            .registerModule(JavaTimeModule())
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    @Bean
    fun consumerFactory(kafkaObjectMapper: ObjectMapper): ConsumerFactory<String, PaymentCompletedEvent> {
        val valueDeserializer = JsonDeserializer(PaymentCompletedEvent::class.java, kafkaObjectMapper)
        valueDeserializer.setRemoveTypeHeaders(false)
        valueDeserializer.setUseTypeMapperForKey(false)
        valueDeserializer.addTrustedPackages("io.cauri.payments.worker.events")

        val props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "payments-worker",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        )

        return DefaultKafkaConsumerFactory(
            props,
            StringDeserializer(),
            ErrorHandlingDeserializer(valueDeserializer),
        )
    }

    @Bean
    fun kafkaListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, PaymentCompletedEvent>,
    ): ConcurrentKafkaListenerContainerFactory<String, PaymentCompletedEvent> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, PaymentCompletedEvent>()
        factory.consumerFactory = consumerFactory
        // Ack por record recién cuando el listener retorna sin excepción. Si SendMessage a SQS
        // falla (NotificationQueue.enqueue relanza), el offset no se commitea y Kafka reintenta
        // indefinidamente el mismo record (spec payments-worker: "el offset no avanza").
        factory.containerProperties.ackMode = ContainerProperties.AckMode.RECORD
        factory.setCommonErrorHandler(DefaultErrorHandler(FixedBackOff(2_000L, FixedBackOff.UNLIMITED_ATTEMPTS)))
        return factory
    }
}
