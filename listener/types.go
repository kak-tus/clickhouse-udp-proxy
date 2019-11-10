package listener

import (
	"sync"

	"github.com/kak-tus/ami"
	"go.uber.org/zap"
)

// Listener object
type Listener struct {
	logger *zap.SugaredLogger
	config Config
	m      *sync.Mutex
	pr     *ami.Producer
	stop   bool
}

type Config struct {
	Port              string
	Redis             redisConfig
	ShardsCount       int8
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
