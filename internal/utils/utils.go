package utils

import (
	"fmt"
	"sync"
	"time"
)

func init() {
	globalPool = NewPool(16)
}

type Task interface {
	Execute()
}

type Pool struct {
	mu    sync.Mutex
	size  int
	tasks chan Task
	kill  chan struct{}
	wg    sync.WaitGroup
}

var globalPool *Pool

func NewPool(size int) *Pool {
	pool := &Pool{
		tasks: make(chan Task, 128),
		kill:  make(chan struct{}),
	}
	pool.Resize(size)
	return pool
}

func (p *Pool) Resize(size int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if size < 0 {
		size = 0
	}
	for size > p.size {
		p.size++
		p.wg.Add(1)
		go p.worker()
	}
	for size < p.size {
		p.size--
		p.kill <- struct{}{}
	}
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for {
		select {
		case task, ok := <-p.tasks:
			if !ok {
				return
			}
			task.Execute()
		case <-p.kill:
			return
		}
	}
}

func (p *Pool) Close() {
	close(p.tasks)
}

func (p *Pool) Wait() {
	p.wg.Wait()
}

func (p *Pool) Exec(task Task) {
	p.tasks <- task
}

func DoWithTries(fn func() error, attempts int, delay time.Duration) error {
	for attempts > 0 {
		if err := fn(); err == nil {
			return nil
		}
		attempts--
		time.Sleep(delay)
	}
	return fmt.Errorf("failed after %d attempts", attempts)
}
