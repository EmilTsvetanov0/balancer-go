package main

import (
	"balancer/internal/balancer"
	cconfig "balancer/internal/config"
	"balancer/internal/limits"
	"balancer/internal/postgresql"
	pgxpool "balancer/internal/postgresql/client"
	"balancer/internal/server"
	"context"
	"errors"
	"github.com/go-chi/chi/v5"
	"github.com/spf13/viper"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func init() {
	cconfig.InitConfig()
}

func main() {

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		cancel()
	}()

	// Balancer

	algo := viper.GetString("balancer.algo")
	servers := viper.GetStringSlice("balancer.servers")
	state := balancer.BaseState(servers)

	bal := balancer.NewBalancer(algo, state)
	logCh := make(chan string)
	checkPath := viper.GetString("balancer.check_path")

	wg := sync.WaitGroup{}

	logger := log.Default()

	wg.Add(1)
	go func() {
		defer wg.Done()
		balancer.SetupLogging(ctx, logger, logCh)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		balancer.HealthMonitor(ctx, state, 5*time.Second, checkPath, logCh)
	}()

	//PostgreSQL
	pgxPool, err := pgxpool.NewClient(context.Background())

	if err != nil {
		log.Fatal(err)
		return
	}

	pgClient := postgresql.NewPgClient(pgxPool, logger)

	// Limiter default config
	defaultMaxKeys := viper.GetInt("limits.default_max_keys")
	defaultRefillRate := viper.GetInt("limits.default_refill_rate")
	limiter := limits.NewLimit(defaultMaxKeys, defaultRefillRate)

	// Server start
	router := chi.NewRouter()

	service := server.New(ctx, pgClient, limiter, bal, server.WithLogger(logger))

	service.SetupRoutes(router)

	port := os.Getenv("PORT")
	logger.Printf("listening on port %s", port)

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: router,
	}

	go func() {
		logger.Printf("Initializing listening")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("listen error: %v", err)
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Printf("server shutdown error: %v", err)
	}

	wg.Wait()
}
