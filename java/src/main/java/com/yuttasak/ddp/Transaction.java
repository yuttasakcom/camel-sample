package com.yuttasak.ddp;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Bank transaction payload produced by the Go service (go/cmd/main.go).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Transaction(
        String transactionId,
        String type,
        String fromAccount,
        String toAccount,
        BigDecimal amount,
        String currency,
        String channel,
        String status,
        Instant timestamp) {
}
