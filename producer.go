package main

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open channel")
	defer ch.Close()

	// 1. Declare a Topic Exchange
	exchangeName := "topic_logs"
	err = ch.ExchangeDeclare(
		exchangeName, // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare topic exchange")

	// 2. Declare two queues
	authQueue, err := ch.QueueDeclare("auth_service_queue", false, false, false, false, nil)
	failOnError(err, "Failed to declare auth queue")

	criticalQueue, err := ch.QueueDeclare("critical_alerts_queue", false, false, false, false, nil)
	failOnError(err, "Failed to declare critical queue")

	// 3. Bind queues using Topic wildcard patterns
	// 'auth_service_queue' listens to any event from the auth service: "auth.*" (e.g. auth.info, auth.error)
	err = ch.QueueBind(authQueue.Name, "auth.*", exchangeName, false, nil)
	failOnError(err, "Failed to bind auth queue")

	// 'critical_alerts_queue' listens to any critical/error event across all services: "#.error"
	err = ch.QueueBind(criticalQueue.Name, "#.error", exchangeName, false, nil)
	failOnError(err, "Failed to bind critical queue")

	// 4. Publish topic messages
	publishTopicMessage(ch, exchangeName, "auth.info", "User John logged in successfully.")
	publishTopicMessage(ch, exchangeName, "auth.error", "Failed login attempt for user Admin.")
	publishTopicMessage(ch, exchangeName, "payment.gateway.error", "Payment provider API timeout.")

	log.Println("[Topic Exchange] Messages published successfully.")
}

func publishTopicMessage(ch *amqp.Channel, exchange, routingKey, body string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ch.PublishWithContext(ctx,
		exchange,   // exchange
		routingKey, // topic routing key
		false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	failOnError(err, "Failed to publish message")
	log.Printf(" [Topic] Sent '%s': '%s'", routingKey, body)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
