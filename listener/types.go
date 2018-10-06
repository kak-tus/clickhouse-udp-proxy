package listener

import (
	"sync"

	"git.aqq.me/go/nanachi"
	"go.uber.org/zap"
)

// Listener object
type Listener struct {
	logger *zap.SugaredLogger
	config listenerConfig
	m      *sync.Mutex
	client *nanachi.Client
	stop   bool
}

type listenerConfig struct {
	Rabbit rabbitConfig
	Port   string
}

type rabbitConfig struct {
	QueueName string
	MaxShard  int32
	URI       string
}

type reqType struct {
	Query   string        `json:"query"`
	Data    []interface{} `json:"data"`
	Types   []string      `json:"types"`
	Version int           `json:"version"`
}
