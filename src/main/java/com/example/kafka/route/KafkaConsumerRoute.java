package com.example.kafka.route;

import com.example.kafka.processor.MessageProcessor;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaManualCommit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Main Kafka consumer route.
 *
 * ┌──────────────────────────────────────────────────────────────────────────┐
 * │  PARALLELISM STRATEGY — consumersCount only                              │
 * │                                                                          │
 * │  Each KafkaConsumer instance (Kafka client) is NOT thread-safe and       │
 * │  MUST stay on a single thread from poll() → process() → commitSync().    │
 * │  This is a hard rule of the Kafka client — not a Camel limitation.       │
 * │                                                                          │
 * │  consumersCount=N  creates N independent KafkaConsumer instances,        │
 * │  each running on its own dedicated thread and owning its own             │
 * │  partition assignment. This is the correct parallelism lever.            │
 * │                                                                          │
 * │  WHY NOT threads() DSL here?                                             │
 * │  Adding .threads() after from("kafka:...") hands the exchange to a       │
 * │  pool thread. This breaks manual offset commits in two ways:             │
 * │    1. commitSync() called from a non-consumer thread → undefined         │
 * │       behaviour in the Kafka client.                                     │
 * │    2. Out-of-order commits: if msg-3 finishes before msg-1, committing   │
 * │       offset-3 silently marks msg-1 and msg-2 as done. On a restart,    │
 * │       those messages are lost forever.                                   │
 * │                                                                          │
 * │  Rule of thumb:                                                          │
 * │    consumersCount ≤ number of topic partitions                           │
 * │    (extra consumers above partition count sit idle)                      │
 * └──────────────────────────────────────────────────────────────────────────┘
 *
 * OFFSET COMMIT STRATEGY — manual sync commit:
 *   autoCommitEnable=false  → Camel does NOT auto-commit
 *   allowManualCommit=true  → KafkaManualCommit header is populated
 *   After successful processing the consumer thread calls commitSync(),
 *   guaranteeing at-least-once delivery semantics.
 *
 * ERROR HANDLING:
 *   - Up to maxRedeliveries retries with exponential back-off.
 *   - On exhaustion the message is forwarded to the DLQ topic.
 *   - Offset is NOT committed for failed messages (they will be
 *     re-delivered after a consumer restart if needed).
 */
@Component
public class KafkaConsumerRoute extends RouteBuilder {

    // ── Kafka connection ───────────────────────────────────────────────────
    @Value("${kafka.brokers}")
    private String brokers;

    @Value("${kafka.input.topic}")
    private String inputTopic;

    @Value("${kafka.consumer.group}")
    private String consumerGroup;

    // ── Parallelism ────────────────────────────────────────────────────────
    // Set this equal to the number of topic partitions.
    // More consumers than partitions = idle threads (no benefit).
    @Value("${kafka.consumers.count:5}")
    private int consumersCount;

    // ── Offset / commit settings ───────────────────────────────────────────
    @Value("${kafka.auto.offset.reset:earliest}")
    private String autoOffsetReset;

    // Keep false — we do manual commitSync() after processing
    @Value("${kafka.enable.auto.commit:false}")
    private boolean enableAutoCommit;

    // ── Error handling ─────────────────────────────────────────────────────
    @Value("${error.max.redeliveries:3}")
    private int maxRedeliveries;

    @Value("${error.redelivery.delay.ms:2000}")
    private long redeliveryDelay;

    @Value("${error.backoff.multiplier:2.0}")
    private double backoffMultiplier;

    @Autowired
    private MessageProcessor messageProcessor;

    // ──────────────────────────────────────────────────────────────────────

    @Override
    public void configure() {

        // ── Exception / DLQ routing ────────────────────────────────────────
        // Retries happen on the SAME consumer thread — safe for Kafka.
        // After exhaustion the exchange is routed to direct:dlq.
        onException(Exception.class)
            .handled(true)
            .maximumRedeliveries(maxRedeliveries)
            .redeliveryDelay(redeliveryDelay)
            .backOffMultiplier(backoffMultiplier)
            .useExponentialBackOff()
            .retryAttemptedLogLevel(LoggingLevel.WARN)
            .retriesExhaustedLogLevel(LoggingLevel.ERROR)
            .log(LoggingLevel.ERROR,
                 "All retries exhausted — routing to DLQ: ${exception.message}")
            .to("direct:dlq");
            // NOTE: offset is NOT committed here — message will be
            // re-delivered if the consumer restarts.

        // ── Kafka URI ─────────────────────────────────────────────────────
        //
        //   consumersCount   → N independent KafkaConsumer threads (parallelism)
        //   autoCommitEnable → false: Camel will not auto-commit
        //   allowManualCommit→ true: populates KafkaManualCommit header
        //
        String kafkaUri = String.format(
            "kafka:%s"
            + "?brokers=%s"
            + "&groupId=%s"
            + "&consumersCount=%d"
            + "&autoOffsetReset=%s"
            + "&autoCommitEnable=%b"
            + "&allowManualCommit=true"
            + "&autoCommitIntervalMs=5000",
            inputTopic,
            brokers,
            consumerGroup,
            consumersCount,
            autoOffsetReset,
            enableAutoCommit
        );

        from(kafkaUri)
            .routeId("kafka-consumer-route")

            // Everything below runs on the SAME consumer thread that called poll().
            // This is the only correct model when using manual offset commits.

            .log(LoggingLevel.DEBUG,
                 "Received | topic=${header[kafka.TOPIC]} "
                 + "partition=${header[kafka.PARTITION]} "
                 + "offset=${header[kafka.OFFSET]} "
                 + "thread=${threadName}")

            // ── Business logic (same consumer thread) ─────────────────────
            .process(messageProcessor)

            // ── Manual offset commit (same consumer thread) ───────────────
            // commitSync() is called on the consumer thread that owns this
            // partition — this is the ONLY safe way to commit with the
            // Kafka client. Committing from any other thread is undefined.
            .process(exchange -> {
                KafkaManualCommit commit =
                    exchange.getIn().getHeader(
                        KafkaConstants.MANUAL_COMMIT,
                        KafkaManualCommit.class);
                if (commit != null) {
                    commit.commitSync();
                    log.debug("Offset committed | partition={} offset={}",
                        exchange.getIn().getHeader("kafka.PARTITION"),
                        exchange.getIn().getHeader("kafka.OFFSET"));
                }
            })

            .log(LoggingLevel.INFO,
                 "Done | partition=${header[kafka.PARTITION]} "
                 + "offset=${header[kafka.OFFSET]} "
                 + "thread=${threadName}");
    }
}
