package balancer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Balancer interface {
	Next() (string, error)
	StartRequest(server string)
	EndRequest(server string)
	SetServerStatus(server string, alive bool)
}

type State struct {
	Servers []string
	Alive   map[string]bool
	mu      sync.RWMutex
}

func BaseState(servers []string) *State {
	alive := make(map[string]bool)
	for _, s := range servers {
		alive[s] = false
	}
	return &State{
		Servers: servers,
		Alive:   alive,
		mu:      sync.RWMutex{},
	}
}

func (s *State) SetServerStatus(server string, alive bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Alive[server] != alive {
		s.Alive[server] = alive
	}
}

// --- Errors ---

var ErrNoAvailableServers = errors.New("no available servers")

// --- RoundRobin ---

type RoundRobin struct {
	state *State
	index int
	mu    sync.Mutex
}

func (rr *RoundRobin) Next() (string, error) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	n := len(rr.state.Servers)
	if n == 0 {
		return "", ErrNoAvailableServers
	}
	for i := 0; i < n; i++ {
		rr.index = (rr.index + 1) % n
		srv := rr.state.Servers[rr.index]
		rr.state.mu.RLock()
		alive := rr.state.Alive[srv]
		rr.state.mu.RUnlock()
		if alive {
			return srv, nil
		}
	}
	return "", ErrNoAvailableServers
}

func (rr *RoundRobin) StartRequest(server string) {}
func (rr *RoundRobin) EndRequest(server string)   {}

func (rr *RoundRobin) SetServerStatus(server string, alive bool) {
	rr.state.SetServerStatus(server, alive)
}

// --- Least connections ---

type LeastConnections struct {
	state       *State
	connections map[string]*int64
}

func (lc *LeastConnections) Next() (string, error) {
	lc.state.mu.RLock()
	defer lc.state.mu.RUnlock()

	var chosen string
	minConn := int64(math.MaxInt64)
	for _, srv := range lc.state.Servers {
		if !lc.state.Alive[srv] {
			continue
		}
		count := atomic.LoadInt64(lc.connections[srv])
		if count < minConn {
			minConn = count
			chosen = srv
		}
	}
	if chosen == "" {
		return "", ErrNoAvailableServers
	}
	return chosen, nil
}
func (lc *LeastConnections) StartRequest(srv string) {
	atomic.AddInt64(lc.connections[srv], 1)
}
func (lc *LeastConnections) EndRequest(srv string) {
	atomic.AddInt64(lc.connections[srv], -1)
}

func (lc *LeastConnections) SetServerStatus(server string, alive bool) {
	lc.state.SetServerStatus(server, alive)
}

// --- Random ---

type Random struct {
	state *State
	rng   *rand.Rand
	mu    sync.Mutex
}

func (r *Random) Next() (string, error) {
	r.state.mu.RLock()
	defer r.state.mu.RUnlock()

	alive := make([]string, 0, len(r.state.Servers))

	for _, s := range r.state.Servers {
		if r.state.Alive[s] {
			alive = append(alive, s)
		}
	}

	if len(alive) == 0 {
		return "", ErrNoAvailableServers
	}

	r.mu.Lock()
	idx := r.rng.Intn(len(alive))
	r.mu.Unlock()

	return alive[idx], nil
}

func (r *Random) StartRequest(server string) {}
func (r *Random) EndRequest(server string)   {}

func (r *Random) SetServerStatus(server string, alive bool) {
	r.state.SetServerStatus(server, alive)
}

// --- New ---

func NewBalancer(algo string, state *State) Balancer {
	switch algo {
	case "round-robin":
		return &RoundRobin{
			state: state,
			index: 0,
		}
	case "least-connections":
		return &LeastConnections{
			state:       state,
			connections: make(map[string]*int64),
		}
	case "random":
		return &Random{
			state: state,
			rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
		}
	default:
		panic("unknown algorithm: " + algo)
	}
}

// --- Health monitoring ---

func SetupLogging(ctx context.Context, logger *log.Logger, logCh <-chan string) {
	logger.Printf("balancer[SetupLogging]: starting logging from %v", time.Now())
	for {
		select {
		case <-ctx.Done():
			logger.Printf("balancer[SetupLogging]: context canceled, shutting down")
			return
		case msg := <-logCh:
			logger.Printf("%s", msg)
		}
	}
}

func HealthMonitor(ctx context.Context, state *State, checkInterval time.Duration, checkPath string, logCh chan<- string) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	client := &http.Client{Timeout: 1 * time.Second}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		logCh <- "balancer[HealthMonitor]: Sending health check request"

		// В теории сервера не меняются, но на всякий случай лучше обновлять. Может быть добавится функционал
		state.mu.RLock()
		servers := append([]string(nil), state.Servers...)
		state.mu.RUnlock()

		var wg sync.WaitGroup
		for _, server := range servers {
			wg.Add(1)
			go func(srv string) {
				defer wg.Done()

				state.mu.RLock()
				alive := state.Alive[srv]
				state.mu.RUnlock()

				CheckHealth(client, state, srv, checkPath, alive, logCh)
			}(server)
		}

		wg.Wait()
	}
}

func CheckHealth(client *http.Client, state *State, server, path string, wasAlive bool, logCh chan<- string) {
	url := fmt.Sprintf("http://%s%s", server, path)

	resp, err := client.Get(url)
	if err != nil {
		if wasAlive {
			state.SetServerStatus(server, false)
			logCh <- fmt.Sprintf("balancer[CheckHealth]: server %s %s is down with error: %v", server, path, err)
		}
		return
	}

	if resp.StatusCode != 200 {
		if wasAlive {
			state.SetServerStatus(server, false)
			logCh <- fmt.Sprintf("balancer[CheckHealth]: server %s %s is down with status code: %d", server, path, resp.StatusCode)
		}
		return
	}

	if !wasAlive {
		state.SetServerStatus(server, true)
		logCh <- fmt.Sprintf("balancer[CheckHealth]: server %s %s is up", server, path)
	}
}
