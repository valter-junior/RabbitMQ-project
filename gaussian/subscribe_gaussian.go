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
	SmallNegative     = "SN"
	VerySmallNegative = "VSN"
	Zero              = "ZE"
	VerySmallPositive = "VSP"
	SmallPositive     = "SP"
	MediumPositive    = "MP"
	LargePositive     = "LP"
	VeryLargePositive = "VLP"

	VeryLargeDecrease = "VLD"
	LargeDecrease     = "LD"
	MediumDecrease    = "MD"
	SmallDecrease     = "SD"
	VerySmallDecrease = "VSD"
	Maintain          = "MAINTAIN"
	VerySmallIncrease = "VSI"
	SmallIncrease     = "SI"
	MediumIncrease    = "MI"
	LargeIncrease     = "LI"
	VeryLargeIncrease = "VLI"

	/*LargeIncrease = "LI"
	SmallIncrease = "SI"
	Maintain      = "MAINTAIN"
	SmallDecrease = "SD"
	LargeDecrease = "LD"*/
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

/*func calculateSigma(currentPeak, nextPeak float64) float64 {

	midpoint := (currentPeak + nextPeak) / 2
	return math.Abs(currentPeak - midpoint)
}*/

func calculateSigma(leftPeak, rightPeak float64) float64 {
	return math.Abs(rightPeak-leftPeak) / 2
}

func fuzzyficationMsgSecInput(msgSec float64) map[string]float64 {
	fuzzy := make(map[string]float64)

	peaks := []float64{
		-15000,
		-10000,
		-5000,
		-2500,
		-1250,
		0,
		1250,
		2500,
		5000,
		10000,
		15000,
	}

	sigmas := make([]float64, len(peaks)-1)
	for i := range sigmas {
		sigmas[i] = calculateSigma(peaks[i], peaks[i+1])
	}

	fuzzy[VeryLargeNegative] = gaussianMembership(msgSec, -15000, sigmas[0])
	fuzzy[LargeNegative] = gaussianMembership(msgSec, -10000, sigmas[1])
	fuzzy[MediumNegative] = gaussianMembership(msgSec, -5000, sigmas[2])
	fuzzy[SmallNegative] = gaussianMembership(msgSec, -2500, sigmas[3])
	fuzzy[VerySmallNegative] = gaussianMembership(msgSec, -1250, sigmas[4])
	fuzzy[Zero] = gaussianMembership(msgSec, 0, sigmas[5])
	fuzzy[VerySmallPositive] = gaussianMembership(msgSec, 1250, sigmas[6])
	fuzzy[SmallPositive] = gaussianMembership(msgSec, 2500, sigmas[7])
	fuzzy[MediumPositive] = gaussianMembership(msgSec, 5000, sigmas[8])
	fuzzy[LargePositive] = gaussianMembership(msgSec, 10000, sigmas[9])
	fuzzy[VeryLargePositive] = gaussianMembership(msgSec, 15000, sigmas[9])

	return fuzzy
}

func fuzzyficationOutput(n float64) map[string]float64 {

	r := map[string]float64{}

	peaks := []float64{
		-8,
		-6,
		-4,
		-2,
		-1,
		0,
		1,
		2,
		4,
		6,
		8,
	}

	sigmas := make([]float64, len(peaks)-1)

	for i := range sigmas {
		sigmas[i] = calculateSigma(peaks[i], peaks[i+1])

	}

	r[VeryLargeDecrease] = gaussianMembership(n, -8, sigmas[0])
	r[LargeDecrease] = gaussianMembership(n, -6, sigmas[1])
	r[MediumDecrease] = gaussianMembership(n, -4, sigmas[2])
	r[SmallDecrease] = gaussianMembership(n, -2, sigmas[3])
	r[VerySmallDecrease] = gaussianMembership(n, -1, sigmas[4])
	r[Maintain] = gaussianMembership(n, 0, sigmas[5])
	r[VerySmallIncrease] = gaussianMembership(n, 1, sigmas[6])
	r[SmallIncrease] = gaussianMembership(n, 2, sigmas[7])
	r[MediumIncrease] = gaussianMembership(n, 4, sigmas[8])
	r[LargeIncrease] = gaussianMembership(n, 6, sigmas[9])
	r[VeryLargeIncrease] = gaussianMembership(n, 8, sigmas[9])

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
		{VerySmallNegative, VerySmallDecrease},
		{Zero, Maintain},
		{VerySmallPositive, VerySmallIncrease},
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

	for i := -8.0; i <= 8.0; i += 0.5 { // Decreased step size for more precision
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

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	gols := 10000
	tickerGols := time.NewTicker(900 * time.Second)
	defer tickerGols.Stop()

	messageReceived := make(chan bool)

	file, err := os.OpenFile("message_rate_gaussian_1.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
					rate := float64(messageCount) / 30
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
