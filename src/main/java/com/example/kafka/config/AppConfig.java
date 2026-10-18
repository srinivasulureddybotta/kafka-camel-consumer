package com.example.kafka.config;

import org.springframework.context.annotation.Configuration;

/**
 * Application configuration.
 *
 * NOTE: No thread pool executor bean is defined here.
 *
 * The parallelism for Kafka consumption is achieved solely via
 * consumersCount on the Kafka URI — which creates N independent
 * KafkaConsumer instances, each on its own dedicated thread.
 *
 * A shared ExecutorService / threadPoolProfile is intentionally
 * NOT used in the consumer route because:
 *
 *   The Kafka client requires that poll(), message processing, and
 *   commitSync() all happen on the SAME consumer thread.
 *   Handing off to a thread pool (threads() DSL) breaks manual
 *   offset commits and risks out-of-order commits and message loss.
 *
 * If you need additional thread pools for non-Kafka work (e.g. a
 * downstream HTTP call that is independent of offset commit), define
 * them here and inject them into the relevant processor beans.
 */
@Configuration
public class AppConfig {
    // Add application-wide beans here as needed.
}
