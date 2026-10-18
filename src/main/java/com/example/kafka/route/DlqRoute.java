package com.example.kafka.route;

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Dead Letter Queue (DLQ) route.
 *
 * Any message that exhausts all redelivery attempts in KafkaConsumerRoute
 * is forwarded here via "direct:dlq".
 *
 * The route enriches the message with failure metadata headers and
 * publishes it to the configured DLQ Kafka topic so it can be inspected,
 * replayed, or alerted on later.
 */
@Component
public class DlqRoute extends RouteBuilder {

    @Value("${kafka.brokers}")
    private String brokers;

    @Value("${kafka.dlq.topic}")
    private String dlqTopic;

    @Override
    public void configure() {

        from("direct:dlq")
            .routeId("dlq-route")
            .log(LoggingLevel.ERROR,
                 "DLQ | topic=${header[kafka.TOPIC]} "
                 + "partition=${header[kafka.PARTITION]} "
                 + "offset=${header[kafka.OFFSET]} "
                 + "error=${exception.message}")

            // Stamp failure details onto the message as headers
            .setHeader("dlq.originalTopic",    header("kafka.TOPIC"))
            .setHeader("dlq.originalPartition", header("kafka.PARTITION"))
            .setHeader("dlq.originalOffset",    header("kafka.OFFSET"))
            .setHeader("dlq.failedAt",          simple("${date:now:yyyy-MM-dd'T'HH:mm:ssZ}"))
            .setHeader("dlq.errorMessage",      simple("${exception.message}"))

            // Publish to the DLQ Kafka topic
            .toF("kafka:%s?brokers=%s", dlqTopic, brokers)

            .log(LoggingLevel.WARN,
                 "Message published to DLQ topic [%s] | "
                 + "originalOffset=${header[dlq.originalOffset]}", dlqTopic);
    }
}
