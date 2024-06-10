package main

import (
	"fmt"
	"log"
	"math"
	"time"

	"github.com/streadway/amqp"
)

var (
	messageCount     = 0
	firstMessageTime = time.Now().Truncate(time.Second)
	lastMessageTime  = time.Now().Truncate(time.Second)
)

const (
	LARGENEGATIVE  = "LN" // Large Negative
	MEDIUMNEGATIVE = "MN" // Medium Negative
	SMALLNEGATIVE  = "SN" // Small Negative
	ZERO           = "ZE" // Zero
	SMALLPOSITIVE  = "SP" // Small Positive
	MEDIUMPOSITIVE = "MP" // Medium Positive
	LARGEPOSITIVE  = "LP" // Large Positive

	LARGEINCREASE = "LI"       // Large Positive
	SMALLINCREASE = "SI"       // Small Positive
	MAINTAIN      = "MAINTAIN" // Zero
	SMALLDECREASE = "SD"       // Small Negative
	LARGEDECREASE = "LD"       // Large Negative

)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func triangularMF(x float64, a float64, b float64, c float64) float64 {
	return math.Max(0, math.Min((x-a)/(b-a), (c-x)/(c-b)))
}

func fuzzyficationMsgSecInput(msgSec float64) map[string]float64 {
	fuzzy := make(map[string]float64)
	fuzzy[LARGENEGATIVE] = triangularMF(msgSec, -20000, -10000, -2500)
	fuzzy[MEDIUMNEGATIVE] = triangularMF(msgSec, -5000, -2500, 0)
	fuzzy[SMALLNEGATIVE] = triangularMF(msgSec, -1500, -500, 0)
	fuzzy[ZERO] = triangularMF(msgSec, -250, 0, 250)
	fuzzy[SMALLPOSITIVE] = triangularMF(msgSec, 0, 500, 1500)
	fuzzy[MEDIUMPOSITIVE] = triangularMF(msgSec, 0, 2500, 5000)
	fuzzy[LARGEPOSITIVE] = triangularMF(msgSec, 2500, 10000, 20000)
	return fuzzy
}

func fuzzyficationOutput(n float64) map[string]float64 {

	r := map[string]float64{}

	r[LARGEINCREASE] = triangularMF(n, 2.0, 3.0, 4.0)
	r[SMALLINCREASE] = triangularMF(n, 1.0, 2.0, 3.0)
	r[MAINTAIN] = triangularMF(n, -1.0, 0.0, 1.0)
	r[SMALLDECREASE] = triangularMF(n, -3.0, -2.0, -1.0)
	r[LARGEDECREASE] = triangularMF(n, -4.0, -3.0, -2.0)

	return r
}

func applyRules(e map[string]float64) ([]float64, []float64) {
	mx := []float64{}
	output := []float64{}

	// Rule 1: IF e = LARGEPOSITIVE THEN output = LARGEINCREASE
	m1 := e[LARGEPOSITIVE]
	o1 := getMaxOutput(LARGEINCREASE)
	mx = append(mx, m1)
	output = append(output, o1)

	// Rule 2: IF e = MEDIUMPOSITIVE THEN output = LARGEINCREASE
	m2 := e[MEDIUMPOSITIVE]
	o2 := getMaxOutput(LARGEINCREASE)
	mx = append(mx, m2)
	output = append(output, o2)

	// Rule 3: IF e = SMALLPOSITIVE THEN output = SMALLINCREASE
	m3 := e[SMALLPOSITIVE]
	o3 := getMaxOutput(SMALLINCREASE)
	mx = append(mx, m3)
	output = append(output, o3)

	// Rule 4: IF e = ZERO THEN output = MAINTAIN
	m4 := e[ZERO]
	o4 := getMaxOutput(MAINTAIN)
	mx = append(mx, m4)
	output = append(output, o4)

	// Rule 5: IF e = SMALLNEGATIVE THEN output = SMALLDECREASE
	m5 := e[SMALLNEGATIVE]
	o5 := getMaxOutput(SMALLDECREASE)
	mx = append(mx, m5)
	output = append(output, o5)

	// Rule 6: IF e = MEDIUMNEGATIVE THEN output = LARGEDECREASE
	m6 := e[MEDIUMNEGATIVE]
	o6 := getMaxOutput(LARGEDECREASE)
	mx = append(mx, m6)
	output = append(output, o6)

	// Rule 7: IF e = LARGENEGATIVE THEN output = LARGEDECREASE
	m7 := e[LARGENEGATIVE]
	o7 := getMaxOutput(LARGEDECREASE)
	mx = append(mx, m7)
	output = append(output, o7)

	return mx, output
}

func getMaxOutput(s string) float64 {
	r := 0.0
	max := -20000.0 // Initialize to a sufficiently low number to ensure any higher value is chosen.

	for i := -4.0; i <= 4.0; i += 0.5 { // Decreased step size for more precision
		v := fuzzyficationOutput(i)

		if v[s] > max {
			max = v[s]
			r = i
		}
	}
	return r
}

func centroidDeffuzification(mx, output, importanceFactors []float64) float64 {
	numerator, denominator := 0.0, 0.0

	for i := 0; i < len(mx); i++ {
		adjustedOutput := output[i] * importanceFactors[i]
		numerator += mx[i] * adjustedOutput
		denominator += mx[i]
		fmt.Printf("Membership: %.4f, Adjusted Output: %.4f, Product: %.4f\n", mx[i], adjustedOutput, mx[i]*adjustedOutput)
	}

	fmt.Printf("Numerator: %.4f, Denominator: %.4f\n", numerator, denominator)
	if denominator == 0 {
		fmt.Println("Warning: Denominator is zero, defaulting output to 1")
		return 1 // You may choose a different default behavior
	}
	return numerator / denominator
}

func Result(p ...float64) float64 {
	goal := p[0]
	rate := p[1]

	e := goal - rate

	fuzzifiedSetError := fuzzyficationMsgSecInput(e)

	log.Printf("goal: %v", goal)

	log.Printf("Fuzzified Error: %v", fuzzifiedSetError)

	// apply rules
	mx, output := applyRules(fuzzifiedSetError)

	// Deffuzification
	importanceFactors := []float64{1.5, 1.5, 1.0, 1.0, 0.5, 0.5, 0.3}
	u := centroidDeffuzification(mx, output, importanceFactors)

	fmt.Printf("Fuzzy Controller: %.2f\n", u)
	return u
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.Qos(5, 0, true)
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

	ticker := time.NewTicker(10 * time.Second)
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
				if time.Since(lastMessageTime) > 10*time.Second && messageCount > 0 {

					log.Printf("firstMessageTime: %v", firstMessageTime)
					log.Printf("lastMessageTime: %v", lastMessageTime)
					duration := int(lastMessageTime.Sub(firstMessageTime).Seconds())
					log.Printf("Messages processed: %d", messageCount)
					log.Printf("Duration: %d", duration)

					rate := float64(messageCount) / float64(duration)

					log.Printf("Rate: %.2f msg/sec", rate)
					Result(10000, float64(rate))

					if duration == 0 {
						continue
					}

					messageCount = 0
					duration = 0
					firstMessageTime = time.Time{}

				}
			case <-messageReceived:
				if firstMessageTime.IsZero() {
					firstMessageTime = time.Now().Truncate(time.Second)
				}
			}
		}

	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	forever := make(chan bool)

	<-forever

}
