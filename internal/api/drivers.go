package api

import (
	"encoding/json"
	"net/http"
	"time"
)

type driver struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Phone    string `json:"phone"`
	Licence  string `json:"licence"`
	Status   string `json:"status"`
	Vehicles int    `json:"vehicles"`
}

func (s *Server) listDrivers(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	rows, err := s.db.QueryContext(r.Context(), `
		SELECT d.id, d.name, COALESCE(d.phone, ''), COALESCE(d.licence, ''), d.status,
		       count(v.id) FILTER (WHERE v.disabled_at IS NULL)
		FROM drivers d
		LEFT JOIN vehicles v ON v.current_driver_id = d.id
		WHERE d.tenant_id = $1
		GROUP BY d.id
		ORDER BY d.name
	`, user.TenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not list drivers")
		return
	}
	defer rows.Close()

	out := []driver{}
	for rows.Next() {
		var d driver
		if rows.Scan(&d.ID, &d.Name, &d.Phone, &d.Licence, &d.Status, &d.Vehicles) == nil {
			out = append(out, d)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": out})
}

type auditEntry struct {
	ID        int64           `json:"id"`
	Action    string          `json:"action"`
	Entity    string          `json:"entity"`
	EntityID  string          `json:"entity_id"`
	UserName  string          `json:"user_name"`
	Metadata  json.RawMessage `json:"metadata"`
	CreatedAt string          `json:"created_at"`
}

func (s *Server) listAudit(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	limit, offset := s.page(r)
	rows, err := s.db.QueryContext(r.Context(), `
		SELECT a.id, a.action, COALESCE(a.entity, ''), COALESCE(a.entity_id, ''),
		       COALESCE(u.name, ''), a.metadata, a.created_at
		FROM audit_log a
		LEFT JOIN users u ON u.id = a.user_id
		WHERE a.tenant_id = $1
		ORDER BY a.created_at DESC
		LIMIT $2 OFFSET $3
	`, user.TenantID, limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not list audit entries")
		return
	}
	defer rows.Close()

	out := []auditEntry{}
	for rows.Next() {
		var e auditEntry
		var meta []byte
		var created time.Time
		if rows.Scan(&e.ID, &e.Action, &e.Entity, &e.EntityID, &e.UserName, &meta, &created) == nil {
			e.Metadata = json.RawMessage(meta)
			e.CreatedAt = created.UTC().Format(time.RFC3339)
			out = append(out, e)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": out})
}
