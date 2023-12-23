package main

import (
	"fmt"
	"log"

	"github.com/dorianneto/kafka-notify/pkg/models"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/requestid"
)

const (
	ProducerPort = ":8080"
)

func sendMessageHandler(users []models.User) fiber.Handler {
	return func(c *fiber.Ctx) error {
		return c.SendString("Hello, World 👋!")
	}
}

func main() {
	users := []models.User{
		{ID: 1, Name: "Emma"},
		{ID: 2, Name: "Bruno"},
		{ID: 3, Name: "Rick"},
		{ID: 4, Name: "Lena"},
	}

	app := fiber.New()
	app.Use(requestid.New())
	app.Use(logger.New(logger.Config{
		Format: "${pid} ${locals:requestid} ${status} - ${method} ${path}​\n",
	}))

	app.Post("/send", sendMessageHandler(users))

	fmt.Printf("Kafka PRODUCER 📨 started at http://localhost%s\n", ProducerPort)

	if err := app.Listen(ProducerPort); err != nil {
		log.Printf("failed to run the server: %v", err)
	}
}
