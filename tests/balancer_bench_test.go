package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type testConfig struct {
	name     string
	capacity int
	rate     int
}

func BenchmarkBalancer(b *testing.B) {
	tests := []testConfig{
		{"no_limit", 1000000000, 1000000000},
		{"limited_10", 10, 0},
		{"limited_100", 100, 100},
	}

	for _, cfg := range tests {
		b.Run(cfg.name, func(b *testing.B) {
			runBenchmark(b, cfg)
		})
	}
}

func runBenchmark(b *testing.B, cfg testConfig) {
	url := "http://localhost:8080/api/ping"
	apiKey := "emik"
	client := &http.Client{}

	// --- Установка лимита ---
	limitBody := fmt.Sprintf(`{"id": "emik", "capacity": %d, "rate": %d}`, cfg.capacity, cfg.rate)
	req, err := http.NewRequest("PUT", "http://localhost:8080/clients", bytes.NewBuffer([]byte(limitBody)))
	if err != nil {
		b.Fatalf("ошибка создания PUT запроса: %v", err)
	}
	req.Header.Set("X-Api-Key", "qwerty")
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		b.Fatalf("ошибка отправки PUT запроса: %v", err)
	}
	resp.Body.Close()

	// --- Подсчёт ответов ---
	counts := map[string]int{
		"M1":  0,
		"M2":  0,
		"M3":  0,
		"429": 0,
	}
	var mu sync.Mutex

	start := time.Now()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				b.Errorf("ошибка создания запроса: %v", err)
				continue
			}
			req.Header.Set("X-Api-Key", apiKey)

			resp, err := client.Do(req)
			if err != nil {
				b.Errorf("ошибка запроса: %v", err)
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			text := string(body)
			mu.Lock()
			switch {
			case strings.Contains(text, "M1"):
				counts["M1"]++
			case strings.Contains(text, "M2"):
				counts["M2"]++
			case strings.Contains(text, "M3"):
				counts["M3"]++
			case strings.Contains(text, "Rate limit exceeded"):
				counts["429"]++
			default:
				b.Errorf("Неожиданный ответ: %s", text)
			}
			mu.Unlock()
		}
	})

	b.StopTimer()

	elapsed := time.Since(start)
	rps := float64(b.N) / elapsed.Seconds()

	fmt.Printf("\n[%s] Результаты:\n", cfg.name)
	fmt.Printf("Ответов от M1: %d\n", counts["M1"])
	fmt.Printf("Ответов от M2: %d\n", counts["M2"])
	fmt.Printf("Ответов от M3: %d\n", counts["M3"])
	fmt.Printf("Ошибок 429: %d\n", counts["429"])
	fmt.Printf("Всего запросов: %d\n", b.N)
	fmt.Printf("Время: %.2fs\n", elapsed.Seconds())
	fmt.Printf("RPS: %.2f req/s\n", rps)
}
