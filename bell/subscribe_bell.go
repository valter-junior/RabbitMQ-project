package main

import (
	"log"
	"math"
	"time"

	"github.com/streadway/amqp"
)

var (
	messageCount     int
	firstMessageTime time.Time
	lastMessageTime  time.Time
)

const (
	LargeNegative  = "LN"
	MediumNegative = "MN"
	SmallNegative  = "SN"
	Zero           = "ZE"
	SmallPositive  = "SP"
	MediumPositive = "MP"
	LargePositive  = "LP"

	LargeIncrease = "LI"
	SmallIncrease = "SI"
	Maintain      = "MAINTAIN"
	SmallDecrease = "SD"
	LargeDecrease = "LD"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func GeneralizedBellMembership(x, a, b, c float64) float64 {
	return 1.0 / (1.0 + math.Pow(math.Abs((x-c)/a), 2*b))
}

func fuzzyficationMsgSecInput(msgSec float64) map[string]float64 {
	fuzzy := make(map[string]float64)
	a := 650.0
	b := 0.87

	centers := map[string]float64{
		LargeNegative:  -10000,
		MediumNegative: -2500,
		SmallNegative:  -500,
		Zero:           0,
		SmallPositive:  500,
		MediumPositive: 2500,
		LargePositive:  10000,
	}

	for label, c := range centers {
		fuzzy[label] = GeneralizedBellMembership(msgSec, a, b, c)
	}

	return fuzzy
}

func fuzzyficationOutput(x float64) map[string]float64 {
	result := make(map[string]float64)
	a, b := 2.0, 2.0
	cValues := map[string]float64{
		LargeDecrease: -4.0,
		SmallDecrease: -2.0,
		Maintain:      0.0,
		SmallIncrease: 2.0,
		LargeIncrease: 4.0,
	}

	for label, c := range cValues {
		width := a
		if label == Maintain {
			width = 0.5 // Narrower for Maintain
		}
		result[label] = GeneralizedBellMembership(x, width, b, c)
	}

	return result
}

func applyRules(e map[string]float64) ([]float64, []float64) {
	mx := []float64{}
	output := []float64{}

	rules := []struct {
		Condition string
		Result    string
	}{
		{LargePositive, LargeIncrease},
		{MediumPositive, LargeIncrease},
		{SmallPositive, SmallIncrease},
		{Zero, Maintain},
		{SmallNegative, SmallDecrease},
		{MediumNegative, LargeDecrease},
		{LargeNegative, LargeDecrease},
	}

	for _, rule := range rules {
		mx = append(mx, e[rule.Condition])
		output = append(output, getMaxOutput(rule.Result))
	}

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
func centroidDefuzzification(mx, output []float64) float64 {
	numerator, denominator := 0.0, 0.0

	for i, m := range mx {
		adjustedOutput := output[i] * m
		numerator += adjustedOutput
		denominator += m // Adjust by importance factors
	}

	if denominator == 0 {
		log.Println("Warning: Denominator is zero, defaulting output to 0")
		return 0
	}
	return numerator / denominator
}

func Result(p ...float64) float64 {
	goal, rate := p[0], p[1]
	e := goal - rate

	fuzzifiedSetError := fuzzyficationMsgSecInput(e)
	log.Printf("goal: %v", goal)
	log.Printf("Fuzzified Error: %v", fuzzifiedSetError)

	mx, output := applyRules(fuzzifiedSetError)
	//importanceFactors := []float64{1.5, 1.5, 1.0, 1.0, 0.5, 0.5, 0.3}

	u := centroidDefuzzification(mx, output)
	log.Printf("Fuzzy Controller: %.2f\n", u)
	return u
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.Qos(14, 0, true)
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

					if duration > 0 {
						rate := float64(messageCount) / float64(duration)
						log.Printf("Rate: %.2f msg/sec", rate)
						Result(30000, rate)
					}
					messageCount = 0
					firstMessageTime = time.Time{}
				}
			case <-messageReceived:
				if firstMessageTime.IsZero() {
					firstMessageTime = time.Now().Truncate(time.Second)
				}
			}
		}
	}()

	log.Println(" [*] Waiting for messages. To exit press CTRL+C")
	forever := make(chan bool)
	<-forever
}
