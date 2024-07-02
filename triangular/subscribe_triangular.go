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

	// Define the overlaps and categories in the range -15000 to 15000
	fuzzy["VLN"] = triangularMF(msgSec, -15000, -15000, -12000) // Start of the range to -12000
	fuzzy["LN"] = triangularMF(msgSec, -13500, -12000, -9000)   // Overlapping midpoint at -12000
	fuzzy["MN"] = triangularMF(msgSec, -10500, -9000, -6000)    // Overlapping midpoint at -9000
	fuzzy["SN"] = triangularMF(msgSec, -7500, -6000, -3000)     // Overlapping midpoint at -6000
	fuzzy["VSN"] = triangularMF(msgSec, -4500, -3000, 0)        // Overlapping midpoint at -3000
	fuzzy["ZE"] = triangularMF(msgSec, -1500, 0, 1500)          // Center at 0
	fuzzy["VSP"] = triangularMF(msgSec, 0, 3000, 4500)          // Overlapping midpoint at 3000
	fuzzy["SP"] = triangularMF(msgSec, 3000, 6000, 7500)        // Overlapping midpoint at 6000
	fuzzy["MP"] = triangularMF(msgSec, 6000, 9000, 10500)       // Overlapping midpoint at 9000
	fuzzy["LP"] = triangularMF(msgSec, 9000, 12000, 13500)      // Overlapping midpoint at 12000
	fuzzy["VLP"] = triangularMF(msgSec, 12000, 15000, 15000)    // End of the range starting from 12000

	return fuzzy
}

func fuzzyficationOutput(n float64) map[string]float64 {
	r := map[string]float64{}

	// Extend and adjust the categories to fit the new range of -8 to 8
	r["VLD"] = triangularMF(n, -8, -7, -6) // New category for broader range
	r["LD"] = triangularMF(n, -7, -6, -5)
	r["MD"] = triangularMF(n, -6, -5, -4)
	r["SD"] = triangularMF(n, -5, -4, -3)
	r["VSD"] = triangularMF(n, -4, -3, -2) // Added for smooth transition
	r["MAINTAIN"] = triangularMF(n, -1, 0, 1)
	r["VSI"] = triangularMF(n, 2, 3, 4) // Added for smooth transition
	r["SI"] = triangularMF(n, 3, 4, 5)
	r["MI"] = triangularMF(n, 4, 5, 6)
	r["LI"] = triangularMF(n, 5, 6, 7)
	r["VLI"] = triangularMF(n, 6, 7, 8) // New category for broader range

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
	//importanceFactors := []float64{1.5, 1.5, 1.0, 1.0, 0.5, 0.5, 0.3}
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

	file, err := os.OpenFile("message_rate_triangular_1.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
