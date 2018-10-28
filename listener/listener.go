package listener

import (
	"net"
	"strings"
	"sync"
	"time"

	"git.aqq.me/go/app/appconf"
	"git.aqq.me/go/app/applog"
	"git.aqq.me/go/app/event"
	"github.com/go-redis/redis"
	"github.com/iph0/conf"
	jsoniter "github.com/json-iterator/go"
	"github.com/kak-tus/ami"
	"github.com/kak-tus/ruthie/message"
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

			addrs := strings.Split(cnf.Redis.Addrs, ",")

			qu, err := ami.NewQu(
				ami.Options{
					Name:              "ruthie",
					Consumer:          cnf.Consumer,
					ShardsCount:       cnf.ShardsCount,
					PrefetchCount:     cnf.PrefetchCount,
					Block:             time.Second,
					PendingBufferSize: cnf.PendingBufferSize,
					PipeBufferSize:    cnf.PipeBufferSize,
					PipePeriod:        time.Microsecond * 10,
				},
				&redis.ClusterOptions{
					Addrs: addrs,
				},
			)
			if err != nil {
				return err
			}

			lstn = &Listener{
				logger: applog.GetLogger().Sugar(),
				m:      &sync.Mutex{},
				config: cnf,
				qu:     qu,
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
			lstn.qu.Close()
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

		err = decoder.Unmarshal(buf[0:num], &parsed)
		if err != nil {
			l.logger.Error(err)
			continue
		}

		body, err := message.Message{
			Query: parsed.Query,
			Data:  parsed.Data,
		}.Encode()

		if err != nil {
			l.logger.Error(err)
			continue
		}

		l.qu.Send(body)
	}

	conn.Close()
	l.m.Unlock()
}
