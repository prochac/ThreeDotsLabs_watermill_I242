package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	streadwayAmqp "github.com/streadway/amqp"
)

func check(err error, msg ...string) {
	if err != nil {
		panic(fmt.Sprintf("%v %v", msg, err))
	}
}

const (
	amqpURI   = "amqp://guest:guest@localhost:5672/"
	queueName = "fokume"
)

func prepareQueue(amqpConfig amqp.Config) {
	conn, err := streadwayAmqp.Dial(amqpConfig.Connection.AmqpURI)
	check(err)
	defer conn.Close()
	ch, err := conn.Channel()
	check(err)
	defer ch.Close()

	_, err = ch.QueueDelete(queueName, false, false, false)
	check(err)

	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	check(err)
}

func publishMessages(amqpConfig amqp.Config) {
	pub, err := amqp.NewPublisher(
		amqpConfig,
		watermill.NewStdLogger(false, false),
	)
	check(err)
	defer pub.Close()
	err = pub.Publish(
		queueName,
		message.NewMessage(watermill.NewUUID(), message.Payload("1s")),  // will pass
		message.NewMessage(watermill.NewUUID(), message.Payload("60s")), // won't pass
		message.NewMessage(watermill.NewUUID(), message.Payload("5s")),  // never fetched
	)
	check(err)
}

func runConsumer(ctx context.Context, amqpConfig amqp.Config) {
	sub, err := amqp.NewSubscriber(
		amqpConfig,
		watermill.NewStdLogger(false, false),
	)
	check(err)

	msgs, err := sub.Subscribe(ctx, queueName)
	check(err)

	for msg := range msgs {
		if err := processMsg(msg); err != nil {
			msg.Nack()
			log.Println("message nacked")
			continue
		}
		if !msg.Ack() {
			log.Println("message wasn't acked")
		}
		log.Println("message acked")
	}
}
func processMsg(msg *message.Message) error {
	td, err := time.ParseDuration(string(msg.Payload))
	check(err)

	log.Printf("got message, gonna sleep for %s\n", td)
	time.Sleep(td)
	log.Println("end of sleep")

	return nil
}

// github.com/streadway/amqp/channel.go#L1548
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	amqpConfig := amqp.NewDurableQueueConfig(amqpURI)
	amqpConfig.Publish.Mandatory = true
	amqpConfig.Consume.Consumer = "watermill"
	amqpConfig.Consume.Qos.PrefetchCount = 1

	prepareQueue(amqpConfig)

	publishMessages(amqpConfig)

	runConsumer(ctx, amqpConfig)
}
