package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
)

var (
	pfc              = 50
	messageCount     = 0
	firstMessageTime = time.Now().Truncate(time.Second)
	lastMessageTime  = time.Now().Truncate(time.Second)
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

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	messageReceived := make(chan bool)

	go func() {

		for d := range msg {
			log.Printf("Received a message: %s", d.Body)
			messageCount++
			lastMessageTime = time.Now().Truncate(time.Second)
			messageReceived <- true
			err := d.Ack(false)
			failOnError(err, "Failed to acknowledge message")
		}

	}()

	go func() {
		for {
			select {
			case <-ticker.C:
				logStats()
			case <-messageReceived:
				if time.Since(lastMessageTime) > 30*time.Second {
					logStats()
				}
			}
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	forever := make(chan bool)

	<-forever

}

func logStats() {

	duration := int(lastMessageTime.Sub(firstMessageTime).Seconds())

	file, err := os.OpenFile("message_count_50.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	failOnError(err, "Failed to open file")

	_, err = file.WriteString(fmt.Sprintf("%v: Received %d messages in %d, Rate: %.2f msg/sec\n", lastMessageTime, messageCount, duration, float64(messageCount)/float64(duration)))
	failOnError(err, "Failed to write to file")

	err = file.Close()
	failOnError(err, "Failed to close file")

	log.Printf("Messages processed: %d", messageCount)
	messageCount = 0
	firstMessageTime = lastMessageTime

}
