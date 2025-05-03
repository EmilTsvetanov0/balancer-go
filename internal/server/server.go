package server

import (
	_ "balancer/docs"
	"balancer/internal/balancer"
	"balancer/internal/domain"
	"balancer/internal/limits"
	postgres "balancer/internal/postgresql"
	"context"
	"encoding/json"
	"errors"
	httpSwagger "github.com/swaggo/http-swagger"
	"log"
	"net/http"
	"net/http/httputil"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/spf13/viper"
)

type contextKey string

const serverContextKey contextKey = "backend_server"

// Service хранит зависимости: Postgres, Limiter, Balancer и контекст
type Service struct {
	pg      *postgres.PgClient
	limiter limits.Limiter
	bal     balancer.Balancer
	ctx     context.Context
	logger  *log.Logger
	proxy   *httputil.ReverseProxy
}

// New создаёт Service с заданными зависимостями
type Option func(*Service)

func New(ctx context.Context, pg *postgres.PgClient, limiter limits.Limiter, bal balancer.Balancer, opts ...Option) *Service {
	s := &Service{
		pg:      pg,
		limiter: limiter,
		bal:     bal,
		ctx:     ctx,
		logger:  log.Default(),
	}
	s.proxy = &httputil.ReverseProxy{
		Director: func(req *http.Request) {},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			s.logger.Printf("server[New]: proxy error: %v", err)
			writeError(w, "bad gateway", http.StatusBadGateway)
		},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// WithLogger позволяет задать свой логгер
type OptionSetter func(*Service)

func WithLogger(logger *log.Logger) Option {
	return func(s *Service) {
		s.logger = logger
	}
}

// writeJSON упрощает возврат JSON
func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("server[writeJSON] error: %v", err)
	}
}

func writeError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(domain.ErrorResponse{Code: code, Message: msg})
}

// SetupRoutes настраивает маршруты в переданном Router
func (s *Service) SetupRoutes(r chi.Router) {
	apiKey := viper.GetString("api.key")

	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// CRUD для клиентов — с проверкой API-ключа
	r.Route("/clients", func(r chi.Router) {
		r.Use(APIKeyAuthMiddleware(apiKey))
		r.Get("/{id}", s.GetClientHandler)
		r.Post("/", s.CreateClientHandler)
		r.Put("/", s.UpdateClientHandler)
		r.Delete("/{id}", s.DeleteClientHandler)
	})

	// Проксирование запросов — без проверки API-ключа
	r.Route("/api", func(r chi.Router) {
		r.Use(s.RateLimitAndPickServer())
		r.Handle("/*", http.HandlerFunc(s.ReverseProxyHandler))
	})

	r.Get("/swagger/*", httpSwagger.WrapHandler) // Swagger документация
}

// APIKeyAuthMiddleware защищает эндпоинты CRUD
func APIKeyAuthMiddleware(key string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("X-API-Key") != key {
				log.Printf("unauthorized access from %s", r.RemoteAddr)
				writeError(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// --- CRUD Handlers ---

func (s *Service) GetClientHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	client, err := s.pg.GetClient(s.ctx, id)
	if err != nil {
		if errors.Is(err, postgres.ErrClientNotFound) {
			s.logger.Printf("server[GetClientHandler]: client %s not found", id)
			writeError(w, "client not found", http.StatusNotFound)
		} else {
			s.logger.Printf("server[GetClientHandler]: error fetching client %s: %v", id, err)
			writeError(w, "internal error", http.StatusInternalServerError)
		}
		return
	}
	writeJSON(w, http.StatusOK, client)
}

func (s *Service) CreateClientHandler(w http.ResponseWriter, r *http.Request) {
	var c domain.Client
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		s.logger.Printf("server[CreateClientHandler]: bad create client request: %v", err)
		writeError(w, "invalid request", http.StatusBadRequest)
		return
	}
	if err := s.pg.InsertClient(s.ctx, c); err != nil {
		if errors.Is(err, postgres.ErrClientAlreadyExists) {
			s.logger.Printf("server[CreateClientHandler]: client %s already exists", c.Id)
			writeError(w, "client already exists", http.StatusBadRequest)
		} else {
			s.logger.Printf("server[CreateClientHandler]: could not create client %s: %v", c.Id, err)
			writeError(w, "could not create client", http.StatusInternalServerError)
		}
		return
	}
	s.limiter.SetLimit(c)
	writeJSON(w, http.StatusCreated, c)
}

func (s *Service) UpdateClientHandler(w http.ResponseWriter, r *http.Request) {
	var c domain.Client
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		s.logger.Printf("server[UpdateClientHandler]: bad update client request: %v", err)
		writeError(w, "invalid request", http.StatusBadRequest)
		return
	}
	if err := s.pg.UpdateClient(s.ctx, c); err != nil {
		if errors.Is(err, postgres.ErrClientNotFound) {
			s.logger.Printf("server[UpdateClientHandler]: client %s not found for update", c.Id)
			writeError(w, "client not found", http.StatusNotFound)
		} else {
			s.logger.Printf("server[UpdateClientHandler]: could not update client %s: %v", c.Id, err)
			writeError(w, "could not update client", http.StatusInternalServerError)
		}
		return
	}
	s.limiter.SetLimit(c)
	writeJSON(w, http.StatusOK, c)
}

func (s *Service) DeleteClientHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.pg.DeleteClient(s.ctx, id); err != nil {
		if errors.Is(err, postgres.ErrClientNotFound) {
			s.logger.Printf("server[DeleteClientHandler]: client %s not found for delete", id)
			writeError(w, "client not found", http.StatusNotFound)
		} else {
			s.logger.Printf("server[DeleteClientHandler]: could not delete client %s: %v", id, err)
			writeError(w, "could not delete client", http.StatusInternalServerError)
		}
		return
	}
	s.limiter.ResetLimit(id)
	w.WriteHeader(http.StatusNoContent)
}

func extractClientID(r *http.Request) string {
	return r.Header.Get("X-API-Key")
}

// RateLimitAndPickServer — middleware для rate limiting и выбора бэкенда
func (s *Service) RateLimitAndPickServer() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := extractClientID(r)
			if !s.limiter.Allow(key) {
				s.logger.Printf("server[RateLimitAndPickServer]: rate limit exceeded for key %s", key)
				writeError(w, "Rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			server, err := s.bal.Next()
			if err != nil {
				if errors.Is(err, balancer.ErrNoAvailableServers) {
					s.logger.Printf("server[RateLimitAndPickServer]: no available backends for request from %s", key)
					writeError(w, "No available backends", http.StatusServiceUnavailable)
					return
				}
				s.logger.Printf("server[RateLimitAndPickServer]: error selecting backend: %v", err)
				writeError(w, "internal error", http.StatusInternalServerError)
				return
			}
			s.bal.StartRequest(server)
			defer s.bal.EndRequest(server)

			ctx := context.WithValue(r.Context(), serverContextKey, server)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// ReverseProxyHandler — проксирует запрос на выбранный бэкенд из контекста
func (s *Service) ReverseProxyHandler(w http.ResponseWriter, r *http.Request) {
	val := r.Context().Value(serverContextKey)
	srv, ok := val.(string)
	if !ok {
		s.logger.Printf("server[ReverseProxyHandler]: backend server not set in context")
		writeError(w, "backend not selected", http.StatusInternalServerError)
		return
	}
	s.proxy.Director = func(req *http.Request) {
		req.URL.Scheme = "http"
		req.URL.Host = srv
		req.URL.Path = strings.TrimPrefix(req.URL.Path, "/api")
		if req.URL.Path == "" {
			req.URL.Path = "/"
		}
	}
	s.proxy.ServeHTTP(w, r)
}
