package api

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/mcchukwu/fleet-tracking-backend/internal/config"
	"github.com/mcchukwu/fleet-tracking-backend/internal/identity"
	"github.com/mcchukwu/fleet-tracking-backend/internal/ratelimit"
	"github.com/redis/go-redis/v9"
)

const (
	sessionCookie = "ftb_session"
	csrfCookie    = "ftb_csrf"
	csrfHeader    = "X-CSRF-Token"
)

type Server struct {
	db       *sql.DB
	rdb      *redis.Client
	identity *identity.Service
	log      *slog.Logger
	cfg      config.API
	limiter  *ratelimit.PerIP
}

func New(db *sql.DB, rdb *redis.Client, ids *identity.Service, cfg config.API, log *slog.Logger) *Server {
	return &Server{
		db:       db,
		rdb:      rdb,
		identity: ids,
		log:      log,
		cfg:      cfg,
		limiter:  ratelimit.NewPerIP(5, 10),
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", s.ready)

	mux.HandleFunc("POST /api/v1/auth/login", s.login)
	mux.Handle("POST /api/v1/auth/logout", s.auth(http.HandlerFunc(s.logout)))
	mux.Handle("GET /api/v1/me", s.auth(http.HandlerFunc(s.me)))

	mux.Handle("GET /api/v1/vehicles", s.auth(http.HandlerFunc(s.listVehicles)))
	mux.Handle("GET /api/v1/vehicles/{id}", s.auth(http.HandlerFunc(s.getVehicle)))
	mux.Handle("GET /api/v1/vehicles/{id}/history", s.auth(http.HandlerFunc(s.vehicleHistory)))
	mux.Handle("GET /api/v1/live/snapshot", s.auth(http.HandlerFunc(s.liveSnapshot)))
	mux.Handle("GET /api/v1/geofences", s.auth(http.HandlerFunc(s.listGeofences)))
	mux.Handle("GET /api/v1/system/health", s.auth(http.HandlerFunc(s.systemHealth)))

	mux.Handle("GET /api/v1/trips", s.auth(http.HandlerFunc(s.listTrips)))
	mux.Handle("POST /api/v1/trips", s.auth(s.role(http.HandlerFunc(s.createTrip), identity.RoleAdmin, identity.RoleDispatcher)))
	mux.Handle("PATCH /api/v1/trips/{id}", s.auth(s.role(http.HandlerFunc(s.updateTrip), identity.RoleAdmin, identity.RoleDispatcher)))

	mux.Handle("GET /api/v1/alerts", s.auth(http.HandlerFunc(s.listAlerts)))
	mux.Handle("POST /api/v1/alerts/{id}/ack", s.auth(s.role(http.HandlerFunc(s.ackAlert), identity.RoleAdmin, identity.RoleDispatcher, identity.RoleSecurity)))
	mux.Handle("GET /api/v1/alert-rules", s.auth(http.HandlerFunc(s.listAlertRules)))
	mux.Handle("PATCH /api/v1/alert-rules/{id}", s.auth(s.role(http.HandlerFunc(s.updateAlertRule), identity.RoleAdmin, identity.RoleDispatcher)))

	mux.Handle("POST /api/v1/geofences", s.auth(s.role(http.HandlerFunc(s.createGeofence), identity.RoleAdmin, identity.RoleDispatcher)))
	mux.Handle("PATCH /api/v1/geofences/{id}", s.auth(s.role(http.HandlerFunc(s.updateGeofence), identity.RoleAdmin, identity.RoleDispatcher)))
	mux.Handle("DELETE /api/v1/geofences/{id}", s.auth(s.role(http.HandlerFunc(s.deleteGeofence), identity.RoleAdmin, identity.RoleDispatcher)))

	mux.Handle("GET /api/v1/reports/summary", s.auth(http.HandlerFunc(s.reportSummary)))
	mux.Handle("POST /api/v1/import/vehicles", s.auth(s.role(http.HandlerFunc(s.importVehicles), identity.RoleAdmin, identity.RoleDispatcher)))
	mux.Handle("GET /api/v1/live/ws", s.auth(http.HandlerFunc(s.liveWS)))

	var handler http.Handler = mux
	handler = s.recoverer(handler)
	handler = s.requestLogger(handler)
	handler = http.MaxBytesHandler(handler, 20<<20)
	return handler
}

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, err := s.currentUser(r)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "unauthorized", "login required")
			return
		}
		if isWrite(r.Method) && !validCSRF(r) {
			writeError(w, http.StatusForbidden, "csrf_failed", "csrf validation failed")
			return
		}
		next.ServeHTTP(w, r.WithContext(withUser(r.Context(), user)))
	})
}

func (s *Server) role(next http.Handler, roles ...identity.Role) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := userFrom(r.Context())
		for _, role := range roles {
			if user != nil && user.Role == role {
				next.ServeHTTP(w, r)
				return
			}
		}
		writeError(w, http.StatusForbidden, "forbidden", "your role cannot perform this action")
	})
}

func (s *Server) currentUser(r *http.Request) (*identity.User, error) {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return nil, identity.ErrUnauthorized
	}
	return s.identity.Authenticate(r.Context(), c.Value)
}

func isWrite(method string) bool {
	return method != http.MethodGet && method != http.MethodHead && method != http.MethodOptions
}

func validCSRF(r *http.Request) bool {
	c, err := r.Cookie(csrfCookie)
	if err != nil || c.Value == "" {
		return false
	}
	return subtleCompare(c.Value, r.Header.Get(csrfHeader))
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.rdb.Ping(ctx).Err(); err != nil {
		writeError(w, http.StatusServiceUnavailable, "redis_unavailable", "redis unavailable")
		return
	}
	if err := s.db.PingContext(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "postgres_unavailable", "postgres unavailable")
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("panic", "error", rec, "path", r.URL.Path)
				writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		s.log.Info("request", "method", r.Method, "path", r.URL.Path, "duration_ms", time.Since(start).Milliseconds())
	})
}

func (s *Server) page(r *http.Request) (limit, offset int) {
	limit = 100
	offset = 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := parsePositive(v); err == nil && n <= 500 {
			limit = n
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := parsePositive(v); err == nil {
			offset = n
		}
	}
	return limit, offset
}

func parsePositive(v string) (int, error) {
	n := 0
	for _, c := range v {
		if c < '0' || c > '9' {
			return 0, errInvalid
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}

var errInvalid = &stringError{"invalid"}

type stringError struct{ s string }

func (e *stringError) Error() string { return e.s }

func subtleCompare(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}

type ctxKey string

const userKey ctxKey = "user"

func withUser(ctx context.Context, u *identity.User) context.Context {
	return context.WithValue(ctx, userKey, u)
}

func userFrom(ctx context.Context) *identity.User {
	u, _ := ctx.Value(userKey).(*identity.User)
	return u
}

func bearer(header string) string {
	const prefix = "Bearer "
	if strings.HasPrefix(header, prefix) {
		return strings.TrimSpace(strings.TrimPrefix(header, prefix))
	}
	return ""
}
