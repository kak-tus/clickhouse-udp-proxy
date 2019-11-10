package listener

import (
	"net"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	jsoniter "github.com/json-iterator/go"
	"github.com/kak-tus/ami"
	"github.com/kak-tus/ruthie/message"
	"go.uber.org/zap"
)

func NewListener(cnf Config, log *zap.SugaredLogger) (*Listener, error) {
	addrs := strings.Split(cnf.Redis.Addrs, ",")

	pr, err := ami.NewProducer(
		ami.ProducerOptions{
			Name:              "ruthie",
			ShardsCount:       cnf.ShardsCount,
			PendingBufferSize: cnf.PendingBufferSize,
			PipeBufferSize:    cnf.PipeBufferSize,
			PipePeriod:        time.Microsecond * 10,
		},
		&redis.ClusterOptions{
			Addrs: addrs,
		},
	)
	if err != nil {
		return nil, err
	}

	lstn := &Listener{
		logger: log,
		m:      &sync.Mutex{},
		config: cnf,
		pr:     pr,
	}

	return lstn, nil
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

	l.logger.Info("Started listener")

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

		l.pr.Send(body)
	}

	conn.Close()
	l.m.Unlock()
}

func (l *Listener) Stop() {
	l.logger.Info("Stop listener")

	l.stop = true
	l.m.Lock()
	l.pr.Close()

	l.logger.Info("Stopped listener")
}
