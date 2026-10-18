package com.example.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Application entry point.
 *
 * Spring Boot starts Camel automatically via camel-spring-boot-starter.
 * All RouteBuilder beans on the classpath are picked up and registered
 * into the auto-configured CamelContext.
 */
@SpringBootApplication
public class MainApp {

    public static void main(String[] args) {
        SpringApplication.run(MainApp.class, args);
    }
}
