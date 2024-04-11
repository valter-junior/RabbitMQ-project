package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

var (
	mu           sync.Mutex
	pfc          = 1
	messageCount = 0
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.Qos(pfc, 0, true)
	failOnError(err, "Failed to set QoS")

	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)

	failOnError(err, "Failed to declare a queue")

	msg, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	failOnError(err, "Failed to register a consumer")

	var wg sync.WaitGroup

	go func() {
		for range time.Tick(30 * time.Second) {
			file, err := os.OpenFile("message_count_new_3.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			failOnError(err, "Failed to open file")

			_, err = file.WriteString(fmt.Sprintf("%v: Received %d messages in the last 30 seconds, Rate: %.2f msg/sec\n", time.Now(), messageCount, float64(messageCount)/30.0))
			failOnError(err, "Failed to write to file")

			err = file.Close()
			failOnError(err, "Failed to close file")

			log.Printf("Messages processed: %d", messageCount)
			messageCount = 0

			mu.Lock()
			pfc++
			err = ch.Qos(pfc, 0, true)
			failOnError(err, "Failed to set QoS")
			mu.Unlock()

		}

	}()

	go func() {
		for d := range msg {
			log.Printf("Received a message: %s", d.Body)

			messageCount++

			err := d.Ack(false)
			failOnError(err, "Failed to acknowledge message")

		}
	}()

	wg.Wait()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	forever := make(chan bool)

	<-forever

}
