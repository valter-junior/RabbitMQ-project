package main

import (
	"fmt"
	"log"
	"math"
	"time"

	"github.com/streadway/amqp"
)

var (
	//pfc              = 50
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

	// Rule 1:  IF e = LARGEPOSITIVE THEN output = LARGEINCREASE
	eR := e[LARGEPOSITIVE]

	m1 := eR
	o1 := getMaxOutput(LARGEINCREASE)
	mx = append(mx, m1)
	output = append(output, o1)

	// Rule 2:  IF e = MEDIUMPOSITIVE THEN output = LARGEINCREASE
	eR = e[MEDIUMPOSITIVE]

	m2 := eR
	o2 := getMaxOutput(LARGEINCREASE)
	mx = append(mx, m2)
	output = append(output, o2)

	// Rule 3:  IF e = SMALLPOSITIVE THEN output = SMALLINCREASE
	eR = e[SMALLPOSITIVE]

	m3 := eR
	o3 := getMaxOutput(SMALLINCREASE)
	mx = append(mx, m3)
	output = append(output, o3)

	// Rule 4:  IF e = ZE THEN output = MAINTAIN
	eR = e[ZERO]

	m4 := eR
	o4 := getMaxOutput(MAINTAIN)
	mx = append(mx, m4)
	output = append(output, o4)

	// Rule 5:  IF e = SN THEN output = SMALLPC
	eR = e[SMALLNEGATIVE]

	m5 := eR
	o5 := getMaxOutput(SMALLDECREASE)
	mx = append(mx, m5)
	output = append(output, o5)

	// Rule 6:  IF e = LN THEN output = LARGEPC
	eR = e[MEDIUMNEGATIVE]

	m6 := eR
	o6 := getMaxOutput(LARGEDECREASE)
	mx = append(mx, m6)
	output = append(output, o6)

	// Rule 7:  IF e = EXTREMELYNEGATIVE THEN output = LARGEDECREASE
	eR = e[LARGENEGATIVE]

	m7 := eR
	o7 := getMaxOutput(LARGEDECREASE)
	mx = append(mx, m7)
	output = append(output, o7)

	return mx, output
}

func getMaxOutput(s string) float64 {
	r := 0.0
	max := -20000.0

	for i := -4.0; i <= 5.0; i += 1.0 {
		v := fuzzyficationOutput(i)

		if v[s] > max {
			max = v[s]
			r = i
		}
	}
	return r
}

func centroidDeffuzification(mx, output []float64) float64 {

	numerator := 0.0
	denominator := 0.0

	for i := 0; i < len(mx); i++ {
		numerator = numerator + mx[i]*output[i]
		denominator = denominator + mx[i]
	}
	u := 0.0
	if denominator == 0 {
		u = 1
	} else {
		u = numerator / denominator
	}
	return u
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
	u := centroidDeffuzification(mx, output)

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

	err = ch.Qos(100, 0, true)
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
					Result(20000, float64(rate))

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

/*func logStats() {

	duration := int(lastMessageTime.Sub(firstMessageTime).Seconds())

	file, err := os.OpenFile("message_count.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	failOnError(err, "Failed to open file")

	_, err = file.WriteString(fmt.Sprintf("%v: Received %d messages in %d, Rate: %.2f msg/sec\n", lastMessageTime, messageCount, duration, float64(messageCount)/float64(duration)))
	failOnError(err, "Failed to write to file")

	err = file.Close()
	failOnError(err, "Failed to close file")

	log.Printf("Messages processed: %d", messageCount)
	messageCount = 0
	firstMessageTime = lastMessageTime

}*/
