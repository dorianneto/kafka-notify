package main

import (
	"encoding/json"
	"errors"
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
	ProducerPort       = ":8081"
	KafkaServerAddress = "localhost:9092"
	KafkaTopic         = "notifications"
)

func findUserByID(id int, users []models.User) (models.User, error) {
	for _, user := range users {
		if user.ID == id {
			return user, nil
		}
	}
	return models.User{}, errors.New("user not found")
}

func getIDFromRequest(formValue string, c *fiber.Ctx) (int, error) {
	id, err := strconv.Atoi(c.FormValue(formValue, "0"))
	if err != nil {
		return 0, fmt.Errorf("failed to parse ID from form value %s: %v", formValue, err)
	}

	return id, nil
}

func sendKafkaMessage(producer sarama.SyncProducer, users []models.User, c *fiber.Ctx, fromId, toId int) error {
	message := c.FormValue("message")

	from, err := findUserByID(fromId, users)
	if err != nil {
		return err
	}

	to, err := findUserByID(toId, users)
	if err != nil {
		return err
	}

	notification := models.Notification{
		From:    from,
		To:      to,
		Message: message,
	}

	notificationJson, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to marshal notification: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: KafkaTopic,
		Key:   sarama.StringEncoder(strconv.Itoa(to.ID)),
		Value: sarama.StringEncoder(notificationJson),
	}

	_, _, err = producer.SendMessage(msg)

	return err
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

		err = sendKafkaMessage(producer, users, c, fromId, toId)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"message": err,
			})
		}

		return c.JSON(fiber.Map{
			"message": "Notification sent successfully!",
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
