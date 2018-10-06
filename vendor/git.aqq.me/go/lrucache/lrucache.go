package lrucache

import (
	"container/list"
	"sync"
	"time"
)

// Cache is thread-safe LRU cache with TTL.
type Cache struct {
	config        Config
	storage       map[interface{}]*list.Element
	evictionQueue *list.List
	ticker        *time.Ticker
	stop          chan struct{}
	wg            sync.WaitGroup
	m             *sync.RWMutex
}

// Config is a structure with configuration parameters for LRU cache.
type Config struct {
	MaxEntries          int
	TTL                 time.Duration
	ExpirationPrecision time.Duration
	EvictionNotifier    EvictionNotifier
}

// Entry represents a cache entry.
type Entry struct {
	Key     interface{}
	Value   interface{}
	Expires time.Time
}

// EvictionNotifier is the interface for eviction notifiers.
type EvictionNotifier interface {
	Notify(*Entry)
}

// New method creates a new Cache. If MaxEntries is zero, the cache has no limit
// and it's entries evicted only by TTL. If TTL is zero, the keys will be evicted
// only when maximum size will be reached. If MaxEntries is zero and TTL is zero
// then assumed that eviction is done by the caller.
func New(config Config) *Cache {
	if config.TTL > 0 &&
		config.ExpirationPrecision == 0 {

		config.ExpirationPrecision = time.Second
	}

	return &Cache{
		config: config,
		m:      &sync.RWMutex{},
	}
}

// Set method adds a value to the cache.
func (c *Cache) Set(key, value interface{}) {
	c.m.Lock()
	defer c.m.Unlock()

	if c.storage == nil {
		c.storage = make(map[interface{}]*list.Element)
		c.evictionQueue = list.New()
		c.stop = make(chan struct{})

		if c.config.TTL > 0 {
			c.ticker = time.NewTicker(c.config.ExpirationPrecision)
			c.watch()
		}
	}

	if el, ok := c.storage[key]; ok {
		en := el.Value.(*Entry)
		en.Value = value

		if c.config.TTL > 0 {
			en.Expires = time.Now().Add(c.config.TTL)
		}

		c.evictionQueue.MoveToBack(el)

		return
	}

	en := &Entry{
		Key:   key,
		Value: value,
	}

	if c.config.TTL > 0 {
		en.Expires = time.Now().Add(c.config.TTL)
	}

	el := c.evictionQueue.PushBack(en)
	c.storage[key] = el

	if c.config.MaxEntries > 0 &&
		c.evictionQueue.Len() > c.config.MaxEntries {

		c.deleteOldest()
	}
}

// Get method retrieves a value of the key.
func (c *Cache) Get(key interface{}) (interface{}, bool) {
	c.m.Lock()
	defer c.m.Unlock()

	if c.storage == nil {
		return nil, false
	}

	if el, ok := c.storage[key]; ok {
		en := el.Value.(*Entry)
		en.Expires = time.Now().Add(c.config.TTL)

		c.evictionQueue.MoveToBack(el)

		return en.Value, true
	}

	return nil, false
}

// Touch method updates expiration time of the key.
func (c *Cache) Touch(key interface{}) {
	c.m.Lock()
	defer c.m.Unlock()

	if c.storage == nil {
		return
	}

	if el, ok := c.storage[key]; ok {
		en := el.Value.(*Entry)
		en.Expires = time.Now().Add(c.config.TTL)

		c.evictionQueue.MoveToBack(el)
	}
}

// Exists method checks a key existence without changing its TTL.
func (c *Cache) Exists(key interface{}) bool {
	c.m.RLock()
	defer c.m.RUnlock()

	if c.storage == nil {
		return false
	}

	if _, ok := c.storage[key]; ok {
		return true
	}

	return false
}

// Range method calls f sequentially for each key and value present in the cache.
// If f returns false, range stops the iteration. Range method does not change
// TTL of keys.
func (c *Cache) Range(f func(interface{}, interface{}) bool) {
	c.m.RLock()
	defer c.m.RUnlock()

	for key, el := range c.storage {
		en := el.Value.(*Entry)
		value := en.Value

		if ok := f(key, value); !ok {
			return
		}
	}
}

// Delete method removes the key from the cache.
func (c *Cache) Delete(key interface{}) {
	c.m.Lock()
	defer c.m.Unlock()

	if c.storage == nil {
		return
	}

	if el, ok := c.storage[key]; ok {
		c.delete(el)
	}
}

// DeleteOldest method removes the oldest key from the cache.
func (c *Cache) DeleteOldest() {
	c.m.Lock()
	defer c.m.Unlock()

	if c.storage == nil {
		return
	}

	c.deleteOldest()
}

// Len method returns the cache length.
func (c *Cache) Len() int {
	c.m.RLock()
	defer c.m.RUnlock()

	if c.storage == nil {
		return 0
	}

	return c.evictionQueue.Len()
}

// Clean method removes all keys from the cache.
func (c *Cache) Clean() {
	c.m.Lock()

	if c.storage == nil {
		c.m.Unlock()
		return
	}

	close(c.stop)

	storage := c.storage
	c.storage = nil
	c.evictionQueue = nil

	if c.ticker != nil {
		c.ticker.Stop()
	}

	c.m.Unlock()

	c.wg.Wait()

	for _, el := range storage {
		en := el.Value.(*Entry)
		c.notifyEviction(en)
	}
}

func (c *Cache) watch() {
	c.wg.Add(1)

	go func() {
		defer c.wg.Done()

		for {
			select {
			case <-c.stop:
				return
			case <-c.ticker.C:
				c.deleteExpired()
			}
		}
	}()
}

func (c *Cache) deleteExpired() {
	c.m.Lock()
	defer c.m.Unlock()

	if c.storage == nil {
		return
	}

	for c.evictionQueue.Len() > 0 {
		el := c.evictionQueue.Front()
		en := el.Value.(*Entry)

		if time.Now().Before(en.Expires) {
			return
		}

		c.delete(el)
	}
}

func (c *Cache) deleteOldest() {
	el := c.evictionQueue.Front()

	if el != nil {
		c.delete(el)
	}
}

func (c *Cache) delete(el *list.Element) {
	en := el.Value.(*Entry)
	delete(c.storage, en.Key)
	c.evictionQueue.Remove(el)
	c.notifyEviction(en)
}

func (c *Cache) notifyEviction(en *Entry) {
	if c.config.EvictionNotifier != nil {
		c.config.EvictionNotifier.Notify(en)
	}
}
