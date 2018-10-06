package nanachi

import (
	"strconv"

	"github.com/streadway/amqp"
)

// Send method reliably sends messages to AMQP-server.
func (p *SmartProducer) Send(msg Publishing) {
	p.m.RLock()
	defer p.m.RUnlock()

	if p.closed {
		panic("can't send on closed smart producer")
	}

	p.msgs <- &msg
}

// CanSend method checks, can the producer send a message to specified destination.
func (p *SmartProducer) CanSend(exchange, key string) bool {
	return p.producer.CanSend(exchange, key)
}

// AddDestination method appends or replaces producer destination.
func (p *SmartProducer) AddDestination(dst *Destination) {
	p.producer.AddDestination(dst)
}

// DeleteDestination method deletes producer destination.
func (p *SmartProducer) DeleteDestination(exchange, key string) {
	p.producer.DeleteDestination(exchange, key)
}

// Close method performs correct closure of the smart producer.
func (p *SmartProducer) Close() {
	p.m.Lock()

	if p.closed {
		p.m.Unlock()
		return
	}

	close(p.msgs)
	p.client.releaseSmartProducer(p.id)
	p.closed = true

	p.m.Unlock()

	p.wg.Wait()

	p.producer.Close()
	p.confirmNotifier.Close()

	if p.returnNotifier != nil {
		p.returnNotifier.Close()
	}
}

func (p *SmartProducer) run() {
	p.wg.Add(1)

	go func() {
		defer p.wg.Done()

		for {
			select {
			case msg, ok := <-p.msgs:
				if !ok {
					p.msgs = nil

					if p.confirmNotifier.C != nil && p.hasPending() {
						continue
					}

					return
				}

				p.send(msg)
			case c := <-p.confirmNotifier.C:
				msg, ok := p.pendingBundles[c.CorrelationId]

				if ok {
					delete(p.pendingBundles, c.CorrelationId)
				}

				if c.Ack {
					if p.config.ConfirmNotifier != nil {
						p.config.ConfirmNotifier.Notify(c)
					}

					if p.msgs == nil && !p.hasPending() {
						return
					}
				} else {
					p.send(msg)
				}
			case ret := <-p.returnNotifier.C:
				msg := &Publishing{
					Exchange:   ret.Exchange,
					RoutingKey: ret.RoutingKey,

					Publishing: amqp.Publishing{
						Headers:         ret.Headers,
						ContentType:     ret.ContentType,
						ContentEncoding: ret.ContentEncoding,
						DeliveryMode:    ret.DeliveryMode,
						Priority:        ret.Priority,
						CorrelationId:   ret.CorrelationId,
						ReplyTo:         ret.ReplyTo,
						Expiration:      ret.Expiration,
						MessageId:       ret.MessageId,
						Timestamp:       ret.Timestamp,
						Type:            ret.Type,
						UserId:          ret.UserId,
						AppId:           ret.AppId,
						Body:            ret.Body,
					},
				}

				p.send(msg)
			}
		}
	}()
}

func (p *SmartProducer) send(msg *Publishing) {
	if msg.CorrelationId == "" {
		cid := strconv.FormatUint(p.pubSeq, 10)
		msg.CorrelationId = correlationIdPref + cid
		p.pubSeq++
	}

	p.pendingBundles[msg.CorrelationId] = msg
	err := p.producer.Send(*msg)

	if err != nil {
		delete(p.pendingBundles, msg.CorrelationId)

		if p.config.ConfirmNotifier != nil {
			p.config.ConfirmNotifier.Notify(
				&Confirmation{
					Ack:           false,
					CorrelationId: msg.CorrelationId,
				},
			)
		}
	}
}

func (p *SmartProducer) hasPending() bool {
	return len(p.pendingBundles) > 0
}
