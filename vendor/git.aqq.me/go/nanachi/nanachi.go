package nanachi

import (
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"git.aqq.me/go/lrucache"
	"git.aqq.me/go/retrier"
	"github.com/iph0/merger"
	"github.com/streadway/amqp"
)

// Client holds the connection to AMQP-server and creates producers and consumers.
type Client struct {
	config           ClientConfig
	conn             *connection
	producerSeq      uint64
	smartProducerSeq uint64
	consumerSeq      uint64
	producers        map[uint64]*Producer
	smartProducers   map[uint64]*SmartProducer
	consumers        map[uint64]*Consumer
	retrier          *retrier.Retrier
	closed           bool
	m                *sync.RWMutex
}

// ClientConfig is the client configuration.
type ClientConfig struct {
	URI             string
	SASL            []amqp.Authentication
	ChannelMax      int
	FrameSize       int
	Heartbeat       time.Duration
	TLSClientConfig *tls.Config
	Properties      amqp.Table
	Locale          string
	Dial            func(network, addr string) (net.Conn, error)
	ErrorNotifier   ErrorNotifier
	RetrierConfig   *retrier.Config
}

// Producer is used for sending messages to AMQP-server.
type Producer struct {
	id             uint64
	config         *ProducerConfig
	client         *Client
	conn           *connection
	channel        *amqp.Channel
	destinations   *lrucache.Cache
	pubSeq         uint64
	pendingBundles map[uint64]string
	closed         bool
	m              *sync.RWMutex
	wg             sync.WaitGroup
}

// ProducerConfig is the producer configuration.
type ProducerConfig struct {
	Destinations      []*Destination
	Confirm           bool
	Mandatory         bool
	Immediate         bool
	PendingBufferSize int
	ConfirmNotifier   ConfirmNotifier
	ReturnNotifier    ReturnNotifier
	ErrorNotifier     ErrorNotifier
	CacheConfig       *lrucache.Config
}

// SmartProducer is used for reliably sending messages to AMQP-server.
type SmartProducer struct {
	id              uint64
	config          *SmartProducerConfig
	client          *Client
	producer        *Producer
	pubSeq          uint64
	pendingBundles  map[string]*Publishing
	msgs            chan *Publishing
	confirmNotifier *ConfirmChanNotifier
	returnNotifier  *ReturnChanNotifier
	closed          bool
	m               *sync.RWMutex
	wg              sync.WaitGroup
}

// SmartProducerConfig is the send helper configuration
type SmartProducerConfig struct {
	Destinations      []*Destination
	Confirm           bool
	Mandatory         bool
	Immediate         bool
	PendingBufferSize int
	ConfirmNotifier   ConfirmNotifier
	ErrorNotifier     ErrorNotifier
	CacheConfig       *lrucache.Config
}

// Consumer is used for consuming message from AMQP-server.
type Consumer struct {
	id       uint64
	config   *ConsumerConfig
	client   *Client
	conn     *connection
	channel  *amqp.Channel
	source   *Source
	msgs     chan *Delivery
	cancels  chan string
	retry    chan struct{}
	stop     chan struct{}
	canceled bool
	closed   bool
	m        *sync.Mutex
	cwg      sync.WaitGroup
	wg       sync.WaitGroup
}

// ConsumerConfig is the consumer configuration.
type ConsumerConfig struct {
	Source        *Source
	ConsumerName  string
	PrefetchCount int
	AutoAck       bool
	Exclusive     bool
	NoLocal       bool
	ErrorNotifier ErrorNotifier
	ParseBody     func([]byte) (interface{}, error)
}

// Destination type reperesents destination information for producer.
type Destination struct {
	Exchange   string
	RoutingKey string
	MaxShard   int32
	Declare    func(*amqp.Channel) error
	declared   bool
}

// Source type reperesents source information for consumer.
type Source struct {
	Queue    string
	MaxShard int32
	Declare  func(*amqp.Channel) error
	declared bool
}

// Publishing type represents a message to send to AMQP-server.
type Publishing struct {
	Exchange   string
	RoutingKey string
	amqp.Publishing
}

// Confirmation type represents a publish confirmation received from AMQP-server.
type Confirmation struct {
	Ack           bool
	CorrelationId string
}

// Delivery type represents received message from AMQP-server.
type Delivery struct {
	amqp.Delivery
	ParsedBody interface{}
	ParseErr   error
}

// ConfirmNotifier is the interface for confirmation notifiers.
type ConfirmNotifier interface {
	Notify(*Confirmation)
}

// ReturnNotifier is the interface for return notifiers.
type ReturnNotifier interface {
	Notify(*amqp.Return)
}

// ErrorNotifier is the interface for error notifiers.
type ErrorNotifier interface {
	Notify(error)
}

type connection struct {
	uri           string
	amqpConfig    amqp.Config
	conn          *amqp.Connection
	errorNotifier ErrorNotifier
	m             *sync.Mutex
	wg            sync.WaitGroup
}

const (
	genericPref       = "nanachi."
	consumerNamePref  = genericPref
	correlationIdPref = genericPref
)

var defaultConsumerConfig = ConsumerConfig{
	PrefetchCount: 10,
}

var defaultProducerConfig = SmartProducerConfig{
	PendingBufferSize: 1000,
}

var defaultSmartProducerConfig = SmartProducerConfig{
	PendingBufferSize: 1000,
}

// NewClient method creates new client.
func NewClient(config ClientConfig) (*Client, error) {
	_, err := amqp.ParseURI(config.URI) // validate URL

	if err != nil {
		return nil, err
	}

	conn := &connection{
		uri: config.URI,
		amqpConfig: amqp.Config{
			SASL:            config.SASL,
			ChannelMax:      config.ChannelMax,
			FrameSize:       config.FrameSize,
			Heartbeat:       config.Heartbeat,
			TLSClientConfig: config.TLSClientConfig,
			Properties:      config.Properties,
			Locale:          config.Locale,
			Dial:            config.Dial,
		},
		errorNotifier: config.ErrorNotifier,
		m:             &sync.Mutex{},
	}

	if config.RetrierConfig == nil {
		config.RetrierConfig = &retrier.Config{}
	}
	retrier := retrier.New(*config.RetrierConfig)

	cli := &Client{
		config:           config,
		conn:             conn,
		producerSeq:      1,
		smartProducerSeq: 1,
		consumerSeq:      1,
		producers:        make(map[uint64]*Producer),
		smartProducers:   make(map[uint64]*SmartProducer),
		consumers:        make(map[uint64]*Consumer),
		retrier:          retrier,
		m:                &sync.RWMutex{},
	}

	return cli, nil
}

// NewProducer method creates new producer.
func (c *Client) NewProducer(config ProducerConfig) *Producer {
	c.m.Lock()
	defer c.m.Unlock()

	if c.closed {
		panic("can't create new producer on closed client")
	}

	return c.newProducer(config)
}

// NewSmartProducer method creates new SmartProducer
func (c *Client) NewSmartProducer(config SmartProducerConfig) *SmartProducer {
	c.m.Lock()
	defer c.m.Unlock()

	if c.closed {
		panic("can't create new smart producer on closed client")
	}

	prd := c.newSmartProducer(config)
	prd.run()

	return prd
}

// NewConsumer method creates new consumer.
func (c *Client) NewConsumer(config ConsumerConfig) *Consumer {
	c.m.Lock()
	defer c.m.Unlock()

	if c.closed {
		panic("can't create new consumer on closed client")
	}

	return c.newConsumer(config)
}

// NewChannel method creates new server channel
func (c *Client) NewChannel() (*amqp.Channel, error) {
	c.m.RLock()
	defer c.m.RUnlock()

	if c.closed {
		panic("can't create new channel on closed client")
	}

	ch, err := c.conn.newChannel()

	if err != nil {
		return nil, err
	}

	return ch, nil
}

// Close method performs correct closure of the client.
func (c *Client) Close() {
	c.m.Lock()

	if c.closed {
		c.m.Unlock()
		return
	}

	c.retrier.Stop()

	producers := c.producers
	c.producers = nil

	smartProducers := c.smartProducers
	c.smartProducers = nil

	consumers := c.consumers
	c.consumers = nil

	c.closed = true

	c.m.Unlock()

	for _, h := range smartProducers {
		h.Close()
	}

	for _, p := range producers {
		p.Close()
	}

	for _, cr := range consumers {
		cr.Close()
	}

	c.conn.close()
}

func (c *Client) newProducer(config ProducerConfig) *Producer {
	config = merger.Merge(defaultProducerConfig, config).(ProducerConfig)

	id := c.producerSeq
	c.producerSeq++

	if config.CacheConfig == nil {
		config.CacheConfig = &lrucache.Config{}
	}
	destinations := lrucache.New(*config.CacheConfig)

	for _, dst := range config.Destinations {
		dkey := dstKey(dst.Exchange, dst.RoutingKey)
		destinations.Set(dkey, dst)
	}

	if config.ErrorNotifier == nil {
		config.ErrorNotifier = c.config.ErrorNotifier
	}

	prd := &Producer{
		id:             id,
		config:         &config,
		client:         c,
		conn:           c.conn,
		destinations:   destinations,
		pubSeq:         1,
		pendingBundles: make(map[uint64]string),
		m:              &sync.RWMutex{},
	}

	c.producers[id] = prd

	return prd
}

func (c *Client) newSmartProducer(config SmartProducerConfig) *SmartProducer {
	config = merger.Merge(defaultSmartProducerConfig, config).(SmartProducerConfig)

	id := c.smartProducerSeq
	c.smartProducerSeq++

	cfmNotifier := NewConfirmChanNotifier(config.PendingBufferSize)
	retNotifier := NewReturnChanNotifier(config.PendingBufferSize)

	if config.ErrorNotifier == nil {
		config.ErrorNotifier = c.config.ErrorNotifier
	}

	prdConfig := ProducerConfig{
		Destinations:      config.Destinations,
		Confirm:           config.Confirm,
		Mandatory:         config.Mandatory,
		Immediate:         config.Immediate,
		PendingBufferSize: config.PendingBufferSize,
		CacheConfig:       config.CacheConfig,
		ErrorNotifier:     config.ErrorNotifier,
	}

	if config.Confirm {
		prdConfig.ConfirmNotifier = cfmNotifier
	}

	if config.Mandatory || config.Immediate {
		prdConfig.ReturnNotifier = retNotifier
	}

	prd := c.newProducer(prdConfig)

	sprd := &SmartProducer{
		id:              id,
		config:          &config,
		client:          c,
		producer:        prd,
		pubSeq:          1,
		pendingBundles:  make(map[string]*Publishing),
		msgs:            make(chan *Publishing),
		confirmNotifier: cfmNotifier,
		returnNotifier:  retNotifier,
		m:               &sync.RWMutex{},
	}

	c.smartProducers[id] = sprd

	return sprd
}

func (c *Client) newConsumer(config ConsumerConfig) *Consumer {
	config = merger.Merge(defaultConsumerConfig, config).(ConsumerConfig)

	id := c.consumerSeq
	c.consumerSeq++

	if config.ConsumerName == "" {
		config.ConsumerName = consumerNamePref + strconv.FormatUint(id, 10)
	}

	if config.ErrorNotifier == nil {
		config.ErrorNotifier = c.config.ErrorNotifier
	}

	cns := &Consumer{
		id:     id,
		config: &config,
		client: c,
		conn:   c.conn,
		source: config.Source,
		m:      &sync.Mutex{},
	}

	c.consumers[id] = cns

	return cns
}

func (c *Client) releaseProducer(id uint64) {
	c.m.Lock()
	defer c.m.Unlock()

	delete(c.producers, id)
}

func (c *Client) releaseConsumer(id uint64) {
	c.m.Lock()
	defer c.m.Unlock()

	delete(c.consumers, id)
}

func (c *Client) releaseSmartProducer(id uint64) {
	c.m.Lock()
	defer c.m.Unlock()

	delete(c.smartProducers, id)
}

func dstKey(exchange, key string) string {
	return fmt.Sprintf("%s.%s", exchange, key)
}
