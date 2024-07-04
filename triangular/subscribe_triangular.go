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
	High   = "High"
	Medium = "Medium"
	Low    = "Low"

	LowDecrease   = "LD"
	MediumMantain = "MZ"
	HighIncrease  = "HI"
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

	// Define as três categorias com sobreposição de 50%
	// Considerando o intervalo de -15000 a 15000 para msgSec
	fuzzy["Low"] = triangularMF(msgSec, -15000, -15000, 0) // De -15000 a 0
	fuzzy["Medium"] = triangularMF(msgSec, -7500, 0, 7500) // De -7500 a 7500, com sobreposição de 50%
	fuzzy["High"] = triangularMF(msgSec, 0, 15000, 15000)  // De 0 a 15000

	return fuzzy
}

func fuzzyficationOutput(n float64) map[string]float64 {
	r := make(map[string]float64)

	// Define as três categorias com sobreposição de 50% no intervalo de -8 a 8
	r["LD"] = triangularMF(n, -8, -8, 0) // De -8 a 0
	r["MZ"] = triangularMF(n, -4, 0, 4)  // De -4 a 4, com sobreposição de 50%
	r["HI"] = triangularMF(n, 0, 8, 8)   // De 0 a 8

	return r
}

func applyRules(e map[string]float64) ([]float64, []float64) {
	mx := []float64{}
	output := []float64{}

	rules := []struct {
		Condition string
		Result    string
	}{
		{Low, LowDecrease},
		{Medium, MediumMantain},
		{High, HighIncrease},
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

/*func centroidDefuzzification(mx, output []float64) float64 {
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
}*/

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

	file, err := os.OpenFile("message_rate_triangular_3.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
