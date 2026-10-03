package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

const topic = "transaction-logs"

type Transaction struct {
	TransactionID string    `json:"transactionId"`
	Type          string    `json:"type"`
	FromAccount   string    `json:"fromAccount,omitempty"`
	ToAccount     string    `json:"toAccount,omitempty"`
	Amount        float64   `json:"amount"`
	Currency      string    `json:"currency"`
	Channel       string    `json:"channel"`
	Status        string    `json:"status"`
	Timestamp     time.Time `json:"timestamp"`
}

var (
	txTypes  = []string{"DEPOSIT", "WITHDRAW", "TRANSFER", "PAYMENT"}
	channels = []string{"MOBILE", "ATM", "BRANCH", "INTERNET"}
	statuses = []string{"SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "FAILED", "PENDING"}
	accounts = []string{
		"123-4-56789-0", "234-5-67890-1", "345-6-78901-2",
		"456-7-89012-3", "567-8-90123-4", "678-9-01234-5",
	}
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func randomAccount() string {
	return accounts[rand.Intn(len(accounts))]
}

func newTransaction() Transaction {
	tx := Transaction{
		TransactionID: uuid.NewString(),
		Type:          txTypes[rand.Intn(len(txTypes))],
		Amount:        float64(rand.Intn(5_000_000)+100) / 100, // 1.00 - 50,000.99 THB
		Currency:      "THB",
		Channel:       channels[rand.Intn(len(channels))],
		Status:        statuses[rand.Intn(len(statuses))],
		Timestamp:     time.Now().UTC(),
	}

	switch tx.Type {
	case "DEPOSIT":
		tx.ToAccount = randomAccount()
	case "WITHDRAW", "PAYMENT":
		tx.FromAccount = randomAccount()
	case "TRANSFER":
		tx.FromAccount = randomAccount()
		for tx.ToAccount = randomAccount(); tx.ToAccount == tx.FromAccount; tx.ToAccount = randomAccount() {
		}
	}
	return tx
}

func main() {
	broker := getEnv("KAFKA_BROKER", "localhost:9092")
	interval, err := time.ParseDuration(getEnv("PRODUCE_INTERVAL", "1s"))
	if err != nil {
		log.Fatalf("invalid PRODUCE_INTERVAL: %v", err)
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		Balancer:               &kafka.Hash{}, // same account -> same partition
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("failed to close writer: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("producing to topic %q on %s every %s (Ctrl+C to stop)", topic, broker, interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down")
			return
		case <-ticker.C:
			tx := newTransaction()
			value, err := json.Marshal(tx)
			if err != nil {
				log.Printf("marshal error: %v", err)
				continue
			}

			key := tx.FromAccount
			if key == "" {
				key = tx.ToAccount
			}

			err = writer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(key),
				Value: value,
			})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("write error: %v", err)
				continue
			}
			fmt.Printf("sent %s %-8s %10.2f %s [%s]\n", tx.TransactionID, tx.Type, tx.Amount, tx.Currency, tx.Status)
		}
	}
}
