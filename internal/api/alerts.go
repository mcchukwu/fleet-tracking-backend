package api

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"time"
)

type alert struct {
	ID         string          `json:"id"`
	VehicleID  string          `json:"vehicle_id"`
	Plate      string          `json:"plate"`
	TripID     string          `json:"trip_id,omitempty"`
	Type       string          `json:"type"`
	Severity   string          `json:"severity"`
	Status     string          `json:"status"`
	OccurredAt string          `json:"occurred_at"`
	AckedAt    *string         `json:"acked_at,omitempty"`
	Evidence   json.RawMessage `json:"evidence"`
}

func (s *Server) listAlerts(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	limit, offset := s.page(r)
	status, kind := r.URL.Query().Get("status"), r.URL.Query().Get("type")

	rows, err := s.db.QueryContext(r.Context(), `
		SELECT a.id, a.vehicle_id, COALESCE(v.plate, ''), a.trip_id, a.type, a.severity,
		       a.status, a.occurred_at, a.acked_at, a.evidence
		FROM alerts a
		JOIN vehicles v ON v.id = a.vehicle_id
		WHERE a.tenant_id = $1
		  AND ($2 = '' OR a.status::text = $2)
		  AND ($3 = '' OR a.type::text = $3)
		ORDER BY a.occurred_at DESC
		LIMIT $4 OFFSET $5
	`, user.TenantID, status, kind, limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not list alerts")
		return
	}
	defer rows.Close()

	out := []alert{}
	for rows.Next() {
		var a alert
		var tripID sql.NullString
		var acked sql.NullTime
		var evidence []byte
		if err := rows.Scan(&a.ID, &a.VehicleID, &a.Plate, &tripID, &a.Type, &a.Severity,
			&a.Status, &a.OccurredAt, &acked, &evidence); err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error", "could not read alerts")
			return
		}
		a.TripID = tripID.String
		if acked.Valid {
			v := acked.Time.UTC().Format(time.RFC3339)
			a.AckedAt = &v
		}
		a.Evidence = json.RawMessage(evidence)
		out = append(out, a)
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": out})
}

func (s *Server) ackAlert(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	tag, err := s.db.ExecContext(r.Context(), `
		UPDATE alerts SET status = 'acked', acked_by = $3, acked_at = now()
		WHERE id = $1 AND tenant_id = $2 AND status = 'open'
	`, r.PathValue("id"), user.TenantID, user.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not acknowledge alert")
		return
	}
	if n, _ := tag.RowsAffected(); n == 0 {
		writeError(w, http.StatusNotFound, "not_found", "open alert not found")
		return
	}
	s.audit(r, user.TenantID, user.ID, "alert.ack", "alert", r.PathValue("id"), nil)
	w.WriteHeader(http.StatusNoContent)
}

type alertRule struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Enabled bool            `json:"enabled"`
	Config  json.RawMessage `json:"config"`
}

func (s *Server) listAlertRules(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	rows, err := s.db.QueryContext(r.Context(), `
		SELECT id, type, enabled, config FROM alert_rules WHERE tenant_id = $1 ORDER BY type
	`, user.TenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not list rules")
		return
	}
	defer rows.Close()

	out := []alertRule{}
	for rows.Next() {
		var rule alertRule
		var cfg []byte
		if err := rows.Scan(&rule.ID, &rule.Type, &rule.Enabled, &cfg); err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error", "could not read rules")
			return
		}
		rule.Config = json.RawMessage(cfg)
		out = append(out, rule)
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": out})
}

func (s *Server) updateAlertRule(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	var req struct {
		Enabled *bool           `json:"enabled"`
		Config  json.RawMessage `json:"config"`
	}
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	tag, err := s.db.ExecContext(r.Context(), `
		UPDATE alert_rules SET
			enabled = COALESCE($3, enabled),
			config = COALESCE($4::jsonb, config)
		WHERE id = $1 AND tenant_id = $2
	`, r.PathValue("id"), user.TenantID, req.Enabled, req.Config)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not update rule")
		return
	}
	if n, _ := tag.RowsAffected(); n == 0 {
		writeError(w, http.StatusNotFound, "not_found", "rule not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
