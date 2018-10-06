/*
Package message - message object for Corrie - reliable (with RabbitMQ) Clickhouse writer.

Usage example

	package main

	import (
		"fmt"
		"time"

		"git.aqq.me/go/nanachi"
		"git.aqq.me/go/retrier"
		"github.com/kak-tus/corrie/message"
		"github.com/streadway/amqp"
	)

	type confirmStdoutNotifier struct{}

	type errorStdoutNotifier struct{}

	func main() {
		client, err := nanachi.NewClient(
			nanachi.ClientConfig{
				URI:           "amqp://example:example@example.com:5672/example",
				Heartbeat:     time.Second * 15,
				ErrorNotifier: new(errorStdoutNotifier),
				RetrierConfig: &retrier.Config{
					RetryPolicy: []time.Duration{time.Second},
				},
			},
		)

		if err != nil {
			panic(err)
		}

		queueName := "messages"
		maxShard := 2

		dst := &nanachi.Destination{
			RoutingKey: queueName,
			MaxShard:   int32(maxShard),
			Declare: func(ch *amqp.Channel) error {
				for i := 0; i <= maxShard; i++ {
					shardName := fmt.Sprintf("%s.%d", queueName, i)

					_, err := ch.QueueDeclare(shardName, true, false, false, false, nil)
					if err != nil {
						panic(err)
					}
				}

				return nil
			},
		}

		producer := client.NewSmartProducer(
			nanachi.SmartProducerConfig{
				Destinations:      []*nanachi.Destination{dst},
				Confirm:           true,
				Mandatory:         true,
				PendingBufferSize: 1000,
				ConfirmNotifier:   new(confirmStdoutNotifier),
			},
		)

		body, err := message.Message{
			Query: "INSERT INTO default.test (some_field) VALUES (?);",
			Data:  []interface{}{1},
		}.Encode()

		if err != nil {
			panic(err)
		}

		err = producer.Send(
			nanachi.Publishing{
				RoutingKey: queueName,
				Publishing: amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: "1",
					Body:          body,
					DeliveryMode:  amqp.Persistent,
				},
			},
		)

		if err != nil {
			panic(err)
		}

		client.Close()
	}

	func (n *confirmStdoutNotifier) Notify(c *nanachi.Confirmation) {
		if c.Ack {
			fmt.Println("Delivery succeed:", c.CorrelationId)
		} else {
			fmt.Println("Delivery failed:", c.CorrelationId)
		}
	}

	func (n *errorStdoutNotifier) Notify(err error) {
		fmt.Println("Error:", err)
	}

*/
package message
