package nanachi

import (
	"errors"

	"github.com/streadway/amqp"
)

func (c *connection) newChannel() (*amqp.Channel, error) {
	c.m.Lock()
	defer c.m.Unlock()

	if c.conn == nil {
		conn, err := amqp.DialConfig(c.uri, c.amqpConfig)

		if err != nil {
			return nil, err
		}

		c.conn = conn
		c.listenClose()
	}

	return c.conn.Channel()
}

func (c *connection) close() {
	c.m.Lock()

	if c.conn != nil {
		c.conn.Close()
	}

	c.m.Unlock()

	c.wg.Wait()
}

func (c *connection) listenClose() {
	closes := c.conn.NotifyClose(make(chan *amqp.Error, 1))

	c.wg.Add(1)

	go func() {
		defer c.wg.Done()

		for err := range closes {
			e := errors.New(err.Reason)
			c.notifyError(e)

			c.abort()
		}
	}()
}

func (c *connection) abort() {
	c.m.Lock()
	defer c.m.Unlock()

	c.conn = nil
}

func (c *connection) notifyError(err error) {
	if c.errorNotifier != nil {
		c.errorNotifier.Notify(err)
	}
}
