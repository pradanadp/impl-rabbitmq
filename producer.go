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

	// 1. Declare a Direct Exchange
	exchangeName := "direct_logs"
	err = ch.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare direct exchange")

	// 2. Declare two queues: one for errors, one for all info/warning/error logs
	errQueue, err := ch.QueueDeclare("error_logs_queue", false, false, false, false, nil)
	failOnError(err, "Failed to declare error queue")

	allQueue, err := ch.QueueDeclare("all_logs_queue", false, false, false, false, nil)
	failOnError(err, "Failed to declare all logs queue")

	// 3. Bind queues to the exchange with specific routing keys
	// 'error_logs_queue' only gets messages with routing key 'error'
	err = ch.QueueBind(errQueue.Name, "error", exchangeName, false, nil)
	failOnError(err, "Failed to bind error queue")

	// 'all_logs_queue' gets 'info', 'warning', AND 'error' messages
	for _, severity := range []string{"info", "warning", "error"} {
		err = ch.QueueBind(allQueue.Name, severity, exchangeName, false, nil)
		failOnError(err, "Failed to bind all logs queue")
	}

	// 4. Publish messages with different routing keys
	publishDirectMessage(ch, exchangeName, "info", "System initialized smoothly.")
	publishDirectMessage(ch, exchangeName, "error", "Database connection lost!")

	log.Println("[Direct Exchange] Messages published successfully.")
}

func publishDirectMessage(ch *amqp.Channel, exchange, routingKey, body string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ch.PublishWithContext(ctx,
		exchange,   // exchange
		routingKey, // exact routing key
		false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	failOnError(err, "Failed to publish message")
	log.Printf(" [Direct] Sent '%s': '%s'", routingKey, body)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
