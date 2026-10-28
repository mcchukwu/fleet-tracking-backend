package api

import (
	"net/http"
	"time"
)

func (s *Server) reportSummary(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	to := time.Now().UTC()
	from := to.Add(-7 * 24 * time.Hour)
	if v := r.URL.Query().Get("from"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			from = t
		}
	}
	if v := r.URL.Query().Get("to"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			to = t
		}
	}

	var vehicles, trips, completed, active, alerts, open int
	_ = s.db.QueryRowContext(r.Context(), `SELECT count(*) FROM vehicles WHERE tenant_id = $1 AND disabled_at IS NULL`, user.TenantID).Scan(&vehicles)
	_ = s.db.QueryRowContext(r.Context(), `
		SELECT count(*), count(*) FILTER (WHERE status = 'completed'), count(*) FILTER (WHERE status = 'active')
		FROM trips WHERE tenant_id = $1 AND created_at BETWEEN $2 AND $3
	`, user.TenantID, from, to).Scan(&trips, &completed, &active)
	_ = s.db.QueryRowContext(r.Context(), `
		SELECT count(*), count(*) FILTER (WHERE status = 'open')
		FROM alerts WHERE tenant_id = $1 AND occurred_at BETWEEN $2 AND $3
	`, user.TenantID, from, to).Scan(&alerts, &open)

	type countRow struct {
		Key   string `json:"key"`
		Count int    `json:"count"`
	}
	byType := []countRow{}
	rows, err := s.db.QueryContext(r.Context(), `
		SELECT type::text, count(*) FROM alerts
		WHERE tenant_id = $1 AND occurred_at BETWEEN $2 AND $3
		GROUP BY type ORDER BY count(*) DESC
	`, user.TenantID, from, to)
	if err == nil {
		for rows.Next() {
			var c countRow
			if rows.Scan(&c.Key, &c.Count) == nil {
				byType = append(byType, c)
			}
		}
		rows.Close()
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"from": from, "to": to,
		"vehicles": vehicles, "trips": trips, "completed_trips": completed,
		"active_trips": active, "alerts": alerts, "open_alerts": open,
		"alerts_by_type": byType,
	})
}
