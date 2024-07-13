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
)

const (
	VeryLargeNegative = "VLN"
	LargeNegative     = "LN"
	MediumNegative    = "MN"
	SmallNegative     = "SN"
	Zero              = "ZE"
	SmallPositive     = "SP"
	MediumPositive    = "MP"
	LargePositive     = "LP"
	VeryLargePositive = "VLP"

	VeryLargeDecrease = "VLD"
	LargeDecrease     = "LD"
	MediumDecrease    = "MD"
	SmallDecrease     = "SD"
	Maintain          = "MAINTAIN"
	SmallIncrease     = "SI"
	MediumIncrease    = "MI"
	LargeIncrease     = "LI"
	VeryLargeIncrease = "VLI"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
func gaussianMembership(x, mu, sigma float64) float64 {
	if sigma == 0 {
		if x == mu {
			return 1.0 // Perfect match
		}
		return 0.0 // No match
	}
	return math.Exp(-math.Pow(x-mu, 2) / (2 * math.Pow(sigma, 2)))
}

func calculateSigma(leftPeak, rightPeak float64) float64 {
	return math.Abs(rightPeak-leftPeak) / 2
}

func fuzzyficationMsgSecInput(msgSec float64) map[string]float64 {
	fuzzy := make(map[string]float64)

	peaks := []float64{
		-10000, -5000, -2500, -1250, 0, 1250, 2500, 5000, 10000,
	}

	sigmas := make([]float64, len(peaks))
	for i := range peaks {
		if i == len(peaks)-1 {
			// Aumenta o sigma em 1.5 vezes para os extremos
			sigmas[i] = calculateSigma(peaks[i-1], peaks[i])
		} else {
			// Calcula o sigma normalmente para outros picos
			sigmas[i] = calculateSigma(peaks[i], peaks[i+1])
		}
	}

	fuzzy[VeryLargeNegative] = gaussianMembership(msgSec, peaks[0], sigmas[0])
	fuzzy[LargeNegative] = gaussianMembership(msgSec, peaks[1], sigmas[1])
	fuzzy[MediumNegative] = gaussianMembership(msgSec, peaks[2], sigmas[2])
	fuzzy[SmallNegative] = gaussianMembership(msgSec, peaks[3], sigmas[3])
	fuzzy[Zero] = gaussianMembership(msgSec, peaks[4], sigmas[4])
	fuzzy[SmallPositive] = gaussianMembership(msgSec, peaks[5], sigmas[5])
	fuzzy[MediumPositive] = gaussianMembership(msgSec, peaks[6], sigmas[6])
	fuzzy[LargePositive] = gaussianMembership(msgSec, peaks[7], sigmas[7])
	fuzzy[VeryLargePositive] = gaussianMembership(msgSec, peaks[8], sigmas[7]) // use the same sigma as the last segment

	return fuzzy
}

func fuzzyficationOutput(n float64) map[string]float64 {
	r := make(map[string]float64)

	peaks := []float64{
		-8, -6, -4, -2, 0, 2, 4, 6, 8,
	}

	sigmas := make([]float64, len(peaks))
	for i := range peaks {
		if i == len(peaks)-1 {
			// Aumenta o sigma em 1.5 vezes para os extremos
			sigmas[i] = calculateSigma(peaks[i-1], peaks[i])
		} else {
			// Calcula o sigma normalmente para outros picos
			sigmas[i] = calculateSigma(peaks[i], peaks[i+1])
		}
	}

	r[VeryLargeDecrease] = gaussianMembership(n, peaks[0], sigmas[0])
	r[LargeDecrease] = gaussianMembership(n, peaks[1], sigmas[1])
	r[MediumDecrease] = gaussianMembership(n, peaks[2], sigmas[2])
	r[SmallDecrease] = gaussianMembership(n, peaks[3], sigmas[3])
	r[Maintain] = gaussianMembership(n, peaks[4], sigmas[4])
	r[SmallIncrease] = gaussianMembership(n, peaks[5], sigmas[5])
	r[MediumIncrease] = gaussianMembership(n, peaks[6], sigmas[6])
	r[LargeIncrease] = gaussianMembership(n, peaks[7], sigmas[7])
	r[VeryLargeIncrease] = gaussianMembership(n, peaks[8], sigmas[7]) // use the same sigma as the last segment

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
		{SmallNegative, SmallDecrease},
		{Zero, Maintain},
		{SmallPositive, SmallIncrease},
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

	file, err := os.OpenFile("message_rate_gaussian_9.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
