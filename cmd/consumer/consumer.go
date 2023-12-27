package main

import (
	"errors"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
	"github.com/dorianneto/kafka-notify/pkg/models"
	"github.com/gofiber/fiber"
)

const (
	ConsumerGroup      = "notifications-group"
	ConsumerTopic      = "notifications"
	ConsumerPort       = ":8082"
	KafkaServerAddress = "localhost:9092"
)

type NotificationStore struct {
	data map[string][]models.Notification
	mu   sync.RWMutex
}

func (ns *NotificationStore) Add(userId string, notification models.Notification) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	ns.data[userId] = append(ns.data[userId], notification)
}

func (ns *NotificationStore) Get(userId string) []models.Notification {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	return ns.data[userId]
}

func getUserIdFromRequest(c *fiber.Ctx) (string, error) {
	id := c.Params("userId")
	if id == "" {
		return "", errors.New("user not found")
	}

	return id, nil
}

func initializeConsumerGroup() (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()

	consumerGroup, err := sarama.NewConsumerGroup([]string{KafkaServerAddress}, ConsumerGroup, config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize consumer group: %w", err)
	}

	return consumerGroup, nil
}

func main() {}
