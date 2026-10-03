package com.yuttasak.ddp;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.component.kafka.KafkaConstants;

public class TransactionConsumerRoute extends RouteBuilder {

    @Override
    public void configure() {
        JacksonDataFormat transactionFormat = new JacksonDataFormat(Transaction.class);
        transactionFormat.addModule(new JavaTimeModule());

        errorHandler(deadLetterChannel("log:com.yuttasak.ddp.invalid?level=ERROR&showBody=true&showException=true")
                .useOriginalMessage());

        from("kafka:{{kafka.topic}}"
                + "?brokers={{kafka.brokers}}"
                + "&groupId={{kafka.group-id}}"
                + "&autoOffsetReset={{kafka.auto-offset-reset}}")
                .routeId("consume-ddp-transactions")
                .log(LoggingLevel.DEBUG, "raw message: ${body}")
                .unmarshal(transactionFormat)
                .process(exchange -> {
                    Transaction tx = exchange.getIn().getBody(Transaction.class);
                    exchange.getIn().setHeader("summary", String.format(
                            "partition=%s offset=%s key=%s | %s %-8s %,12.2f %s | from=%s to=%s | %-8s %-7s | %s",
                            exchange.getIn().getHeader(KafkaConstants.PARTITION),
                            exchange.getIn().getHeader(KafkaConstants.OFFSET),
                            exchange.getIn().getHeader(KafkaConstants.KEY),
                            tx.transactionId(),
                            tx.type(),
                            tx.amount(),
                            tx.currency(),
                            orDash(tx.fromAccount()),
                            orDash(tx.toAccount()),
                            tx.channel(),
                            tx.status(),
                            tx.timestamp()));
                })
                .log("${header.summary}");
    }

    private static String orDash(String value) {
        return value == null || value.isBlank() ? "-" : value;
    }
}
