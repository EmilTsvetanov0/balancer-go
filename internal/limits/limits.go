package limits

import (
	"balancer/internal/domain"
	postgres "balancer/internal/postgresql"
	"context"
	"log"
	"sync"
	"time"
)

type Limiter interface {
	Allow(key string) bool
	SetLimit(client domain.Client)
	ResetLimit(key string)
	Stop()
}

type bucket struct {
	tokens     int
	capacity   int
	rate       int
	lastRefill time.Time
}

type IntervalLimiter struct {
	ctx         context.Context
	cancelFunc  context.CancelFunc
	mu          sync.Mutex
	buckets     map[string]*bucket
	defaultCap  int
	defaultRate int
	pg          *postgres.PgClient
	logger      *log.Logger
	ticker      *time.Ticker
	stopChan    chan struct{}
	wg          sync.WaitGroup // Для ожидания завершения горутины
}

type TimeBasedLimiter struct {
	ctx         context.Context
	mu          sync.Mutex
	buckets     map[string]*bucket
	defaultCap  int
	defaultRate int
	pg          *postgres.PgClient
	logger      *log.Logger
}

func NewLimit(ctx context.Context, defaultCap int, defaultRate int, client *postgres.PgClient, logg *log.Logger, limiterType string) Limiter {
	switch limiterType {
	case "interval":
		ctx, cancel := context.WithCancel(ctx)
		l := &IntervalLimiter{
			ctx:         ctx,
			cancelFunc:  cancel,
			buckets:     make(map[string]*bucket),
			defaultCap:  defaultCap,
			defaultRate: defaultRate,
			pg:          client,
			logger:      logg,
			stopChan:    make(chan struct{}),
		}
		l.ticker = time.NewTicker(1 * time.Second)
		l.wg.Add(1)
		go l.refillTokensPeriodically()
		return l
	case "time-based":
		return &TimeBasedLimiter{
			ctx:         ctx,
			buckets:     make(map[string]*bucket),
			defaultCap:  defaultCap,
			defaultRate: defaultRate,
			pg:          client,
			logger:      logg,
		}
	default:
		panic("limits: unknown limiter type: " + limiterType)
	}

}

// Методы для IntervalLimiter

func (l *IntervalLimiter) refillTokensPeriodically() {
	defer l.wg.Done()
	for {
		select {
		case <-l.stopChan:
			l.logger.Println("Stopping token refill")
			return
		case <-l.ticker.C:
			l.mu.Lock()
			for key, b := range l.buckets {
				b.tokens += b.rate
				if b.tokens > b.capacity {
					b.tokens = b.capacity
				}
				l.buckets[key] = b
			}
			l.mu.Unlock()
		}
	}
}

func (l *IntervalLimiter) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[key]
	if !ok {
		l.logger.Printf("limits[Allow]: key %s not found in buckets, updating", key)
		client, err := l.pg.GetClient(l.ctx, key)
		var curTokens int
		var allowed = true
		if err != nil {
			l.logger.Printf("limits[Allow]: setting default limits, error getting client for key %s: %v", key, err)
			curTokens = l.defaultCap - 1
			if curTokens < 0 {
				curTokens = 0
				allowed = false
			}
			l.buckets[key] = &bucket{
				tokens:   curTokens,
				capacity: l.defaultCap,
				rate:     l.defaultRate,
			}
		} else {
			curTokens = client.Capacity - 1
			if curTokens < 0 {
				curTokens = 0
				allowed = false
			}
			l.buckets[key] = &bucket{
				tokens:   curTokens,
				capacity: client.Capacity,
				rate:     client.Rate,
			}
		}
		return allowed
	}

	if b.tokens <= 0 {
		return false
	}
	b.tokens--
	return true
}

func (l *IntervalLimiter) SetLimit(client domain.Client) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.buckets[client.Id] = &bucket{
		tokens:   client.Capacity,
		capacity: client.Capacity,
		rate:     client.Rate,
	}
}

func (l *IntervalLimiter) ResetLimit(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.buckets, key)
}

func (l *IntervalLimiter) Stop() {
	l.cancelFunc()
	close(l.stopChan)
	l.ticker.Stop()
	l.wg.Wait()
	l.logger.Println("limits[Stop]: Limiter stopped")
}

// Методы для TimeBasedLimiter

func (l *TimeBasedLimiter) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	b, ok := l.buckets[key]
	if !ok {
		l.logger.Printf("limits[Allow]: key %s not found in buckets, updating", key)
		client, err := l.pg.GetClient(l.ctx, key)
		var curTokens int
		var allowed = true
		if err != nil {
			l.logger.Printf("limits[Allow]: setting default limits, error getting client for key %s: %v", key, err)
			curTokens = l.defaultCap - 1
			if curTokens < 0 {
				curTokens = 0
				allowed = false
			}
			l.buckets[key] = &bucket{
				tokens:     curTokens,
				capacity:   l.defaultCap,
				rate:       l.defaultRate,
				lastRefill: now,
			}
		} else {
			curTokens = client.Capacity - 1
			if curTokens < 0 {
				curTokens = 0
				allowed = false
			}
			l.buckets[key] = &bucket{
				tokens:     curTokens,
				capacity:   client.Capacity,
				rate:       client.Rate,
				lastRefill: now,
			}
		}
		return allowed
	}

	elapsedSecs := int(now.Sub(b.lastRefill).Seconds())
	refillTokens := elapsedSecs * b.rate

	if refillTokens > 0 {
		b.tokens += refillTokens
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
		b.lastRefill = b.lastRefill.Add(time.Duration(elapsedSecs) * time.Second)
	}

	if b.tokens <= 0 {
		return false
	}
	b.tokens--
	return true
}

func (l *TimeBasedLimiter) SetLimit(client domain.Client) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	l.buckets[client.Id] = &bucket{
		tokens:     client.Capacity,
		capacity:   client.Capacity,
		rate:       client.Rate,
		lastRefill: now,
	}
}

func (l *TimeBasedLimiter) ResetLimit(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.buckets, key)
}

func (l *TimeBasedLimiter) Stop() {
	l.logger.Println("limits[Stop]: Limiter stopped")
}
