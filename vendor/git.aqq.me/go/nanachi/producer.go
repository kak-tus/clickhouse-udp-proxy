package nanachi

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"git.aqq.me/go/retrier"
	"github.com/streadway/amqp"
)

func init() {
	now := time.Now()
	rand.Seed(now.Unix())
}

// Send method sends messages to AMQP-server.
func (p *Producer) Send(msg Publishing) error {
	return p.client.retrier.Do(
		func() *retrier.Error {
			err := p.send(msg)

			if err != nil {
				p.notifyError(err)
				return retrier.NewError(err, false)
			}

			return nil
		},
	)
}

// CanSend method checks, can the producer send a message to specified destination.
func (p *Producer) CanSend(exchange, rkey string) bool {
	dkey := dstKey(exchange, rkey)
	_, ok := p.destinations.Get(dkey)

	if !ok {
		return false
	}

	return true
}

// AddDestination method appends or replaces producer destination.
func (p *Producer) AddDestination(dst *Destination) {
	dkey := dstKey(dst.Exchange, dst.RoutingKey)
	p.destinations.Set(dkey, dst)
}

// DeleteDestination method deletes producer destination.
func (p *Producer) DeleteDestination(exchange, rkey string) {
	dkey := dstKey(exchange, rkey)
	p.destinations.Delete(dkey)
}

// Close method performs correct closure of the producer.
func (p *Producer) Close() {
	p.m.Lock()

	if p.closed {
		p.m.Unlock()
		return
	}

	if p.channel != nil {
		p.channel.Close()
	}

	p.client.releaseProducer(p.id)
	p.closed = true

	p.m.Unlock()

	p.wg.Wait()

	if p.config.ConfirmNotifier != nil {
		if p.config.Confirm && p.hasPending() {
			err := errors.New("producer closed prematurely")
			p.notifyError(err)

			for _, cid := range p.pendingBundles {
				p.config.ConfirmNotifier.Notify(
					&Confirmation{
						Ack:           false,
						CorrelationId: cid,
					},
				)
			}
		}
	}
}

func (p *Producer) send(msg Publishing) error {
	p.m.Lock()
	defer p.m.Unlock()

	if p.closed {
		panic("can't send on closed producer")
	}

	dkey := dstKey(msg.Exchange, msg.RoutingKey)
	iDst, ok := p.destinations.Get(dkey)

	if !ok {
		panic("sending to unknown destination")
	}

	dst := iDst.(*Destination)

	if p.channel == nil {
		err := p.init()

		if err != nil {
			return err
		}
	}

	if !dst.declared {
		if dst.Declare != nil {
			err := dst.Declare(p.channel)

			if err != nil {
				return err
			}
		}

		dst.declared = true
	}

	var pubID uint64

	if p.config.Confirm {
		pubID = p.pubSeq
		p.pubSeq++
		p.pendingBundles[pubID] = msg.CorrelationId
	}

	rkey := dst.RoutingKey

	if dst.MaxShard > 0 {
		if msg.Headers == nil {
			msg.Headers = make(amqp.Table)
		}

		var shardNum int32
		iShardNum, ok := msg.Headers["x-shard"]

		if ok {
			shardNum = iShardNum.(int32)

			if shardNum > dst.MaxShard {
				shardNum = dst.MaxShard
				msg.Headers["x-shard"] = shardNum
			}
		} else {
			shardNum = rand.Int31n(dst.MaxShard + 1)
			msg.Headers["x-shard"] = shardNum
		}

		rkey = shardedQueueName(rkey, shardNum)
	}

	err := p.channel.Publish(
		dst.Exchange,
		rkey,
		p.config.Mandatory,
		p.config.Immediate,
		msg.Publishing,
	)

	if err != nil && p.config.Confirm {
		cid, ok := p.pendingBundles[pubID]

		if ok {
			delete(p.pendingBundles, pubID)

			if p.config.ConfirmNotifier != nil {
				p.config.ConfirmNotifier.Notify(
					&Confirmation{
						Ack:           false,
						CorrelationId: cid,
					},
				)
			}
		}
	}

	return err
}

func (p *Producer) init() error {
	ch, err := p.conn.newChannel()

	if err != nil {
		return err
	}

	p.channel = ch
	p.listenClose()

	if p.config.Confirm {
		err = ch.Confirm(false)

		if err != nil {
			return err
		}

		if p.config.ConfirmNotifier != nil {
			p.listenConfirms()
		}
	}

	if p.config.ReturnNotifier != nil {
		p.listenReturns()
	}

	return nil
}

func (p *Producer) listenClose() {
	closes := p.channel.NotifyClose(make(chan *amqp.Error, 1))

	p.wg.Add(1)

	go func() {
		defer p.wg.Done()

		for err := range closes {
			e := errors.New(err.Reason)
			p.notifyError(e)

			p.abort(err)
		}
	}()
}

func (p *Producer) listenConfirms() {
	confirms := p.channel.NotifyPublish(
		make(chan amqp.Confirmation, p.config.PendingBufferSize))

	p.wg.Add(1)

	go func() {
		defer p.wg.Done()

		for c := range confirms {
			p.m.Lock()

			cid, ok := p.pendingBundles[c.DeliveryTag]

			if ok {
				delete(p.pendingBundles, c.DeliveryTag)

				p.config.ConfirmNotifier.Notify(
					&Confirmation{
						Ack:           c.Ack,
						CorrelationId: cid,
					},
				)
			}

			p.m.Unlock()
		}
	}()
}

func (p *Producer) listenReturns() {
	returns := p.channel.NotifyReturn(
		make(chan amqp.Return, p.config.PendingBufferSize))

	p.wg.Add(1)

	go func() {
		defer p.wg.Done()

		for ret := range returns {
			p.m.Lock()

			dkey := dstKey(ret.Exchange, ret.RoutingKey)
			iDst, ok := p.destinations.Get(dkey)

			if !ok {
				if i := strings.LastIndexByte(ret.RoutingKey, '.'); i >= 0 {
					ret.RoutingKey = ret.RoutingKey[:i]
					dkey := dstKey(ret.Exchange, ret.RoutingKey)
					iDst, ok = p.destinations.Get(dkey)
				}
			}

			if ok {
				dst := iDst.(*Destination)
				dst.declared = false
			}

			p.m.Unlock()

			if p.config.ReturnNotifier != nil {
				p.config.ReturnNotifier.Notify(&ret)
			}
		}
	}()
}

func (p *Producer) abort(err error) {
	p.m.Lock()

	if p.closed {
		p.m.Unlock()
		return
	}

	pendingBundles := p.pendingBundles
	p.pendingBundles = make(map[uint64]string)

	p.channel = nil
	p.pubSeq = 1

	p.destinations.Range(
		func(key, iDst interface{}) bool {
			dst := iDst.(*Destination)
			dst.declared = false

			return true
		},
	)

	p.m.Unlock()

	if p.config.Confirm && p.config.ConfirmNotifier != nil {
		for _, cid := range pendingBundles {
			p.config.ConfirmNotifier.Notify(
				&Confirmation{
					Ack:           false,
					CorrelationId: cid,
				},
			)
		}
	}
}

func (p *Producer) notifyError(err error) {
	if p.config.ErrorNotifier != nil {
		p.config.ErrorNotifier.Notify(err)
	}
}

func (p *Producer) hasPending() bool {
	return len(p.pendingBundles) > 0
}

func shardedQueueName(queueName string, shardNum int32) string {
	return fmt.Sprintf("%s.%d", queueName, shardNum)
}
