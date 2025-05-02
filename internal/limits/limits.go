package limits

import (
	"balancer/internal/domain"
	"sync"
	"time"
)

type Limiter interface {
	Allow(key string) bool
	SetLimit(client domain.Client)
	ResetLimit(key string)
}

type bucket struct {
	tokens     int
	capacity   int
	rate       int
	lastRefill time.Time
}

type Limit struct {
	mu          sync.Mutex
	buckets     map[string]*bucket
	defaultCap  int
	defaultRate int
}

func NewLimit(defaultCap int, defaultRate int) *Limit {
	return &Limit{
		buckets:     make(map[string]*bucket),
		defaultCap:  defaultCap,
		defaultRate: defaultRate,
	}
}

func (l *Limit) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	b, ok := l.buckets[key]
	if !ok {
		l.buckets[key] = &bucket{
			tokens:     l.defaultCap - 1,
			capacity:   l.defaultCap,
			rate:       l.defaultRate,
			lastRefill: now,
		}
		return true
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

func (l *Limit) SetLimit(client domain.Client) {
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

func (l *Limit) ResetLimit(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.buckets, key)
}
