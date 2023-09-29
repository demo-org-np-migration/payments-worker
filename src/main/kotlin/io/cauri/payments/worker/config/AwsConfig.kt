package io.cauri.payments.worker.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsClient
import java.net.URI

/** Cliente SQS contra LocalStack (`AWS_ENDPOINT_URL`) tanto en staging como en prod (las convenciones internas de API
 *  #2). Credenciales `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` salen del entorno, nunca de código. */
@Configuration
class AwsConfig(
    @Value("\${aws.region}") private val region: String,
    @Value("\${aws.endpoint-url}") private val endpointUrl: String,
) {
    @Bean
    fun sqsClient(): SqsClient =
        SqsClient.builder()
            .region(Region.of(region))
            .endpointOverride(URI.create(endpointUrl))
            .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
            .build()
}
