package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

var (
	messageCount = 0
	//firstMessageTime = time.Now().Truncate(time.Second)
	//lastMessageTime  = time.Now().Truncate(time.Second)
)

const (
	VeryLargeNegative = "VLN"
	LargeNegative     = "LN"
	MediumNegative    = "MN"
	Zero              = "ZE"
	MediumPositive    = "MP"
	LargePositive     = "LP"
	VeryLargePositive = "VLP"

	VeryLargeDecrease = "VLD"
	LargeDecrease     = "LD"
	MediumDecrease    = "MD"
	Maintain          = "MAINTAIN"
	MediumIncrease    = "MI"
	LargeIncrease     = "LI"
	VeryLargeIncrease = "VLI"
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

	fuzzy[VeryLargeNegative] = triangularMF(msgSec, -10000, -5000, -2500)
	fuzzy[LargeNegative] = triangularMF(msgSec, -5000, -2500, -1250)
	fuzzy[MediumNegative] = triangularMF(msgSec, -2500, -1250, -625)
	fuzzy[Zero] = triangularMF(msgSec, -625, 0, 625)
	fuzzy[MediumPositive] = triangularMF(msgSec, 625, 1250, 2500)
	fuzzy[LargePositive] = triangularMF(msgSec, 1250, 2500, 5000)
	fuzzy[VeryLargePositive] = triangularMF(msgSec, 2500, 5000, 10000)

	return fuzzy
}

func fuzzyficationOutput(n float64) map[string]float64 {
	r := map[string]float64{}

	r[VeryLargeDecrease] = triangularMF(n, -8, -7, -6)
	r[LargeDecrease] = triangularMF(n, -7, -6, -4)
	r[MediumDecrease] = triangularMF(n, -6, -4, -2)
	r[Maintain] = triangularMF(n, -2, 0, 2)
	r[MediumIncrease] = triangularMF(n, 2, 4, 6)
	r[LargeIncrease] = triangularMF(n, 4, 6, 7)
	r[VeryLargeIncrease] = triangularMF(n, 6, 7, 8)

	return r
}

func applyRules(e map[string]float64) ([]float64, []float64) {
	mx := []float64{}
	output := []float64{}

	rules := []struct {
		Condition string
		Result    string
	}{
		{VeryLargeNegative, VeryLargeDecrease},
		{LargeNegative, LargeDecrease},
		{MediumNegative, MediumDecrease},
		{Zero, Maintain},
		{MediumPositive, MediumIncrease},
		{LargePositive, LargeIncrease},
		{VeryLargePositive, VeryLargeIncrease},
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

	for i := -8.0; i <= 8.0; i += 0.1 { // Decreased step size for more precision
		v := fuzzyficationOutput(i)

		if v[s] > max {
			max = v[s]
			r = i
		}
	}
	return r
}

func centroidDefuzzification(mx []float64, output []float64) float64 {
	if len(mx) != len(output) {
		fmt.Println("Error: membership and output arrays must be of the same length")
		return 0
	}

	var numerator, denominator float64

	for i := range mx {
		numerator += mx[i] * output[i]
		denominator += mx[i]
	}

	if denominator == 0 {
		return 0 // Avoid division by zero
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
	//importanceFactors := []float64{1.5, 1.5, 1.0, 1.0, 0.5, 0.5, 0.3}

	fmt.Printf("mx: %v\n", mx)
	fmt.Printf("output: %v\n", output)
	u := centroidDefuzzification(mx, output)

	fmt.Printf("Fuzzy Controller: %.2f\n", u)
	return u
}

func main() {
	prefetchValue := 1
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	fmt.Println("Enter the prefetch value: ", prefetchValue)

	if prefetchValue == 1 {
		err = ch.Qos(prefetchValue, 0, true)
		failOnError(err, "Failed to set QoS")

	}

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

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	gols := 10000
	tickerGols := time.NewTicker(450 * time.Second)
	defer tickerGols.Stop()

	messageReceived := make(chan bool)

	file, err := os.OpenFile("message_rate_triangular_7_1.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	go func() {
		for d := range msg {
			err := d.Ack(false)
			log.Printf("Received a message: %s", d.Body)
			messageCount++

			messageReceived <- true

			failOnError(err, "Failed to acknowledge message")
		}
	}()

	go func() {
		for {

			select {

			case <-ticker.C:

				if messageCount > 0 {
					rate := float64(messageCount) / 15
					rateAdjust := Result(float64(gols), rate)

					if _, err := file.WriteString(time.Now().Format("2006-01-02 15:04:05") + " - Messages: " + strconv.Itoa(messageCount) + ", Rate: " + fmt.Sprintf("%.2f", rate) + " msg/sec, Prefetch: " + strconv.Itoa(prefetchValue) + " - " + "Prefetch valur adjust: " + strconv.Itoa(int(rateAdjust)) + " - " + "Goal: " + strconv.Itoa(gols) + "\n"); err != nil {
						log.Fatal(err)
					}
					//int(rateAdjust)
					prefetchValue += int(rateAdjust)
					log.Printf("Messages processed in the last 10 seconds: %d", messageCount)
					log.Printf("Current prefetch value: %d", prefetchValue)
					// Escrever no arquivo
					messageCount = 0
					err = ch.Qos(prefetchValue, 0, true)
					failOnError(err, "Failed to set QoS")
				}
			case <-messageReceived:

			case <-tickerGols.C:
				if gols > 15000 && gols < 17000 {
					gols = int(gols / 2)
				} else if gols > 25000 && gols < 28000 {

					gols -= 10000

				} else {
					gols += 4000
				}

			}
		}
	}()

	log.Println(" [*] Waiting for messages. To exit press CTRL+C")
	forever := make(chan bool)
	<-forever

}
