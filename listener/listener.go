package listener

import (
	"fmt"
	"net"
	"sync"
	"time"

	"git.aqq.me/go/app/appconf"
	"git.aqq.me/go/app/applog"
	"git.aqq.me/go/app/event"
	"git.aqq.me/go/nanachi"
	"git.aqq.me/go/retrier"
	"github.com/iph0/conf"
	jsoniter "github.com/json-iterator/go"
	"github.com/streadway/amqp"
)

var lstn *Listener

func init() {
	event.Init.AddHandler(
		func() error {
			cnfMap := appconf.GetConfig()["listener"]

			var cnf listenerConfig
			err := conf.Decode(cnfMap, &cnf)
			if err != nil {
				return err
			}

			lstn = &Listener{
				logger: applog.GetLogger().Sugar(),
				m:      &sync.Mutex{},
				config: cnf,
			}

			lstn.logger.Info("Started listener")

			return nil
		},
	)

	event.Stop.AddHandler(
		func() error {
			lstn.logger.Info("Stop listener")
			lstn.stop = true
			lstn.m.Lock()
			lstn.client.Close()
			lstn.logger.Info("Stopped listener")
			return nil
		},
	)
}

// GetListener return instance
func GetListener() *Listener {
	return lstn
}

// Start listener
func (l *Listener) Start() {
	client, err := nanachi.NewClient(
		nanachi.ClientConfig{
			URI:       l.config.Rabbit.URI,
			Heartbeat: time.Second * 15,
			RetrierConfig: &retrier.Config{
				RetryPolicy: []time.Duration{time.Second},
			},
		},
	)

	if err != nil {
		l.logger.Panic(err)
	}

	dest := &nanachi.Destination{
		RoutingKey: l.config.Rabbit.QueueName,
		MaxShard:   l.config.Rabbit.MaxShard,
		Declare: func(ch *amqp.Channel) error {
			for i := 0; i <= int(l.config.Rabbit.MaxShard); i++ {
				shardName := fmt.Sprintf("%s.%d", l.config.Rabbit.QueueName, i)

				_, err := ch.QueueDeclare(shardName, true, false, false, false, nil)
				if err != nil {
					l.logger.Panic(err)
				}
			}

			return nil
		},
	}

	producer := client.NewSmartProducer(
		nanachi.SmartProducerConfig{
			Destinations:      []*nanachi.Destination{dest},
			Confirm:           true,
			Mandatory:         true,
			PendingBufferSize: 1000,
		},
	)

	l.client = client

	decoder := jsoniter.Config{UseNumber: true}.Froze()

	conn, err := net.ListenPacket("udp", ":"+l.config.Port)
	if err != nil {
		l.logger.Panic(err)
	}

	buf := make([]byte, 65535)
	var parsed reqType

	l.m.Lock()

	for {
		if l.stop {
			break
		}

		err = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		if err != nil {
			continue
		}

		num, _, err := conn.ReadFrom(buf)
		if err != nil {
			continue
		}

		// Unmarshal only to test message for correct format
		// It is compatible with corrie format
		err = decoder.Unmarshal(buf[0:num], &parsed)
		if err != nil {
			l.logger.Error(err)
			continue
		}

		producer.Send(
			nanachi.Publishing{
				RoutingKey: l.config.Rabbit.QueueName,
				Publishing: amqp.Publishing{
					ContentType:  "text/plain",
					Body:         buf[0:num],
					DeliveryMode: amqp.Persistent,
				},
			},
		)
	}

	conn.Close()
	l.m.Unlock()
}
