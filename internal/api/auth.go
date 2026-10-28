package api

import (
	"net"
	"net/http"

	"github.com/mcchukwu/fleet-tracking-backend/internal/identity"
)

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type userResponse struct {
	ID       string `json:"id"`
	TenantID string `json:"tenant_id"`
	Email    string `json:"email"`
	Name     string `json:"name"`
	Role     string `json:"role"`
}

func toUserResponse(u *identity.User) userResponse {
	return userResponse{ID: u.ID, TenantID: u.TenantID, Email: u.Email, Name: u.Name, Role: string(u.Role)}
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	if !s.limiter.Allow(clientIP(r)) {
		writeError(w, http.StatusTooManyRequests, "rate_limited", "too many login attempts")
		return
	}

	var req loginRequest
	if err := decode(r, &req); err != nil || req.Email == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "email and password are required")
		return
	}

	raw, user, err := s.identity.Login(r.Context(), req.Email, req.Password, r.UserAgent(), clientIP(r))
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid_credentials", "invalid email or password")
		return
	}

	csrf, _, err := identity.NewToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	s.setAuthCookies(w, raw, csrf)

	s.audit(r, user.TenantID, user.ID, "auth.login", "user", user.ID, nil)
	writeJSON(w, http.StatusOK, map[string]any{
		"user":       toUserResponse(user),
		"csrf_token": csrf,
	})
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	c, err := r.Cookie(sessionCookie)
	if err == nil {
		_ = s.identity.Logout(r.Context(), c.Value)
	}
	s.clearAuthCookies(w)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	csrf := ""
	if c, err := r.Cookie(csrfCookie); err == nil {
		csrf = c.Value
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"user":       toUserResponse(user),
		"csrf_token": csrf,
	})
}

func (s *Server) setAuthCookies(w http.ResponseWriter, session, csrf string) {
	ttl := int(s.cfg.SessionTTL.Seconds())
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: session, Path: "/", HttpOnly: true,
		Secure: s.cfg.CookieSecure, SameSite: http.SameSiteLaxMode, MaxAge: ttl,
	})
	http.SetCookie(w, &http.Cookie{
		Name: csrfCookie, Value: csrf, Path: "/", HttpOnly: false,
		Secure: s.cfg.CookieSecure, SameSite: http.SameSiteLaxMode, MaxAge: ttl,
	})
}

func (s *Server) clearAuthCookies(w http.ResponseWriter) {
	for _, name := range []string{sessionCookie, csrfCookie} {
		http.SetCookie(w, &http.Cookie{
			Name: name, Value: "", Path: "/", MaxAge: -1,
			HttpOnly: name == sessionCookie, Secure: s.cfg.CookieSecure, SameSite: http.SameSiteLaxMode,
		})
	}
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *Server) audit(r *http.Request, tenantID, userID, action, entity, entityID string, metadata map[string]any) {
	if userID == "" {
		return
	}
	_, err := s.db.ExecContext(r.Context(), `
		INSERT INTO audit_log (tenant_id, user_id, action, entity, entity_id, metadata, ip_address)
		VALUES ($1, $2, $3, $4, $5, COALESCE($6, '{}'::jsonb), $7)
	`, tenantID, userID, action, entity, entityID, metadata, clientIP(r))
	if err != nil {
		s.log.Error("audit write failed", "action", action, "error", err)
	}
}
