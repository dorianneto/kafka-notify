package main

import (
	"fmt"
	"log"
	"strconv"

	"github.com/IBM/sarama"
	"github.com/dorianneto/kafka-notify/pkg/models"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/requestid"
)

const (
	ProducerPort       = ":8080"
	KafkaServerAddress = "localhost:9092"
)

func getIDFromRequest(formValue string, c *fiber.Ctx) (int, error) {
	id, err := strconv.Atoi(c.FormValue(formValue, "0"))
	if err != nil {
		return 0, fmt.Errorf("failed to parse ID from form value %s: %v", formValue, err)
	}

	return id, nil
}

func sendMessageHandler(producer sarama.SyncProducer, users []models.User) fiber.Handler {
	return func(c *fiber.Ctx) error {
		fromId, err := getIDFromRequest("fromId", c)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"message": err,
			})
		}

		toId, err := getIDFromRequest("toId", c)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"message": err,
			})
		}

		return c.JSON(fiber.Map{
			"message": fmt.Sprintf("%v/%v", fromId, toId),
		})
	}
}

func setupProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{KafkaServerAddress}, config)
	if err != nil {
		return nil, fmt.Errorf("failed to setup producer: %w", err)
	}

	return producer, nil
}

func main() {
	users := []models.User{
		{ID: 1, Name: "Emma"},
		{ID: 2, Name: "Bruno"},
		{ID: 3, Name: "Rick"},
		{ID: 4, Name: "Lena"},
	}

	producer, err := setupProducer()
	if err != nil {
		log.Fatalf("failed to initialize producer: %v", err)
	}
	defer producer.Close()

	app := fiber.New()
	app.Use(requestid.New())
	app.Use(logger.New(logger.Config{
		Format: "${pid} ${locals:requestid} ${status} - ${method} ${path}​\n",
	}))

	app.Post("/send", sendMessageHandler(producer, users))

	fmt.Printf("Kafka PRODUCER 📨 started at http://localhost%s\n", ProducerPort)

	if err := app.Listen(ProducerPort); err != nil {
		log.Printf("failed to run the server: %v", err)
	}
}
