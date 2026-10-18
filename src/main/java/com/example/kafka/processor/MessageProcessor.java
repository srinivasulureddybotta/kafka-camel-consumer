package com.example.kafka.processor;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Core business-logic processor.
 *
 * Invoked from KafkaConsumerRoute inside the threads() pool, so it runs
 * concurrently across many threads.  Make sure any shared state accessed
 * here is thread-safe (use thread-local variables, immutable objects, or
 * properly synchronized / concurrent data structures).
 */
@Component
public class MessageProcessor implements Processor {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    @Override
    public void process(Exchange exchange) throws Exception {

        // ── Kafka metadata ────────────────────────────────────────────────
        String topic     = exchange.getIn().getHeader("kafka.TOPIC",     String.class);
        Integer partition = exchange.getIn().getHeader("kafka.PARTITION", Integer.class);
        Long    offset    = exchange.getIn().getHeader("kafka.OFFSET",    Long.class);
        String  key       = exchange.getIn().getHeader("kafka.KEY",       String.class);
        String  threadName = Thread.currentThread().getName();

        // ── Message body ──────────────────────────────────────────────────
        String body = exchange.getIn().getBody(String.class);

        log.debug("[{}] Received message | topic={} partition={} offset={} key={} body={}",
                threadName, topic, partition, offset, key, body);

        // ── TODO: add your business logic here ────────────────────────────
        //
        // Examples:
        //   - Parse JSON:  ObjectMapper om = ...; MyEvent e = om.readValue(body, MyEvent.class);
        //   - Persist:     myRepository.save(e);
        //   - Forward:     exchange.getIn().setBody(transformed);
        //
        // ─────────────────────────────────────────────────────────────────

        // Simulate some processing work
        log.info("[{}] Processed message | topic={} partition={} offset={}",
                threadName, topic, partition, offset);
    }
}
