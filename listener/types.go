package listener

import (
	"sync"

	"github.com/kak-tus/ami"
	"go.uber.org/zap"
)

// Listener object
type Listener struct {
	logger *zap.SugaredLogger
	config listenerConfig
	m      *sync.Mutex
	qu     *ami.Qu
	stop   bool
}

type listenerConfig struct {
	Port              string
	Redis             redisConfig
	Consumer          string
	ShardsCount       int8
	PrefetchCount     int64
	PendingBufferSize int64
	PipeBufferSize    int64
}

type redisConfig struct {
	Addrs string
}

type reqType struct {
	Query   string        `json:"query"`
	Data    []interface{} `json:"data"`
	Types   []string      `json:"types"`
	Version int           `json:"version"`
}
