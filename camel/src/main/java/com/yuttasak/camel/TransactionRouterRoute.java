package com.yuttasak.camel;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;

/**
 * Syncs bank transactions from the source cluster (Go producer's topic)
 * to the sync cluster (topic read by the Java consumer).
 */
public class TransactionRouterRoute extends RouteBuilder {

    @Override
    public void configure() {
        errorHandler(defaultErrorHandler()
                .maximumRedeliveries(3)
                .redeliveryDelay(1000)
                .logExhausted(true));

        from("kafka:{{kafka.source-topic}}"
                + "?brokers={{kafka.source-brokers}}"
                + "&groupId={{kafka.group-id}}"
                + "&autoOffsetReset={{kafka.auto-offset-reset}}")
                .routeId("transaction-logs-to-consumer-ddp")
                // the consumer sets kafka.KEY; the producer reads it back so the account key is preserved
                .log("move partition=${header." + KafkaConstants.PARTITION + "}"
                        + " offset=${header." + KafkaConstants.OFFSET + "}"
                        + " key=${header." + KafkaConstants.KEY + "}"
                        + " -> {{kafka.target-brokers}}/{{kafka.target-topic}}")
                .to("kafka:{{kafka.target-topic}}"
                        + "?brokers={{kafka.target-brokers}}"
                        + "&requestRequiredAcks=all");
    }
}
