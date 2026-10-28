package api

import (
	"database/sql"
	"net/http"
	"time"
)

type trip struct {
	ID          string  `json:"id"`
	VehicleID   string  `json:"vehicle_id"`
	Plate       string  `json:"plate"`
	DriverID    string  `json:"driver_id,omitempty"`
	DriverName  string  `json:"driver_name,omitempty"`
	Origin      string  `json:"origin"`
	Destination string  `json:"destination"`
	Status      string  `json:"status"`
	StartedAt   *string `json:"started_at,omitempty"`
	EndedAt     *string `json:"ended_at,omitempty"`
}

type tripRequest struct {
	VehicleID   string `json:"vehicle_id"`
	DriverID    string `json:"driver_id"`
	Origin      string `json:"origin"`
	Destination string `json:"destination"`
	Status      string `json:"status"`
}

func (s *Server) listTrips(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	limit, offset := s.page(r)
	status := r.URL.Query().Get("status")

	rows, err := s.db.QueryContext(r.Context(), `
		SELECT t.id, t.vehicle_id, COALESCE(v.plate, ''), t.driver_id, COALESCE(d.name, ''),
		       t.origin, t.destination, t.status, t.started_at, t.ended_at
		FROM trips t
		JOIN vehicles v ON v.id = t.vehicle_id
		LEFT JOIN drivers d ON d.id = t.driver_id
		WHERE t.tenant_id = $1 AND ($2 = '' OR t.status::text = $2)
		ORDER BY t.created_at DESC
		LIMIT $3 OFFSET $4
	`, user.TenantID, status, limit, offset)
	if err != nil {
		s.log.Error("list trips failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "could not list trips")
		return
	}
	defer rows.Close()

	out := []trip{}
	for rows.Next() {
		var t trip
		var driverID, driverName sql.NullString
		var started, ended sql.NullTime
		if err := rows.Scan(&t.ID, &t.VehicleID, &t.Plate, &driverID, &driverName,
			&t.Origin, &t.Destination, &t.Status, &started, &ended); err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error", "could not read trips")
			return
		}
		t.DriverID, t.DriverName = driverID.String, driverName.String
		if started.Valid {
			v := started.Time.UTC().Format(time.RFC3339)
			t.StartedAt = &v
		}
		if ended.Valid {
			v := ended.Time.UTC().Format(time.RFC3339)
			t.EndedAt = &v
		}
		out = append(out, t)
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": out})
}

func (s *Server) createTrip(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	var req tripRequest
	if err := decode(r, &req); err != nil || req.VehicleID == "" || req.Origin == "" || req.Destination == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "vehicle_id, origin and destination are required")
		return
	}
	if req.Status == "" {
		req.Status = "planned"
	}
	if !validTripStatus(req.Status) {
		writeError(w, http.StatusBadRequest, "invalid_status", "unknown trip status")
		return
	}

	var id string
	err := s.db.QueryRowContext(r.Context(), `
		INSERT INTO trips (tenant_id, vehicle_id, driver_id, origin, destination, status, started_at, created_by)
		SELECT $1, v.id, NULLIF($3, '')::uuid, $4, $5, $6::trip_status,
		       CASE WHEN $6 = 'active' THEN now() END, $7
		FROM vehicles v
		WHERE v.id = $2 AND v.tenant_id = $1 AND v.disabled_at IS NULL
		RETURNING id
	`, user.TenantID, req.VehicleID, req.DriverID, req.Origin, req.Destination, req.Status, user.ID).Scan(&id)
	if err == sql.ErrNoRows {
		writeError(w, http.StatusNotFound, "vehicle_not_found", "vehicle not found")
		return
	}
	if err != nil {
		s.log.Error("create trip failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "could not create trip")
		return
	}
	s.audit(r, user.TenantID, user.ID, "trip.create", "trip", id, map[string]any{"vehicle_id": req.VehicleID})
	writeJSON(w, http.StatusCreated, map[string]any{"id": id})
}

func (s *Server) updateTrip(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	var req tripRequest
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	if req.Status != "" && !validTripStatus(req.Status) {
		writeError(w, http.StatusBadRequest, "invalid_status", "unknown trip status")
		return
	}

	tag, err := s.db.ExecContext(r.Context(), `
		UPDATE trips SET
			status = COALESCE(NULLIF($3, '')::trip_status, status),
			driver_id = COALESCE(NULLIF($4, '')::uuid, driver_id),
			started_at = CASE WHEN $3 = 'active' AND started_at IS NULL THEN now() ELSE started_at END,
			ended_at = CASE WHEN $3 IN ('completed', 'cancelled') THEN now() ELSE ended_at END,
			updated_at = now()
		WHERE id = $1 AND tenant_id = $2
	`, r.PathValue("id"), user.TenantID, req.Status, req.DriverID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not update trip")
		return
	}
	if n, _ := tag.RowsAffected(); n == 0 {
		writeError(w, http.StatusNotFound, "not_found", "trip not found")
		return
	}
	s.audit(r, user.TenantID, user.ID, "trip.update", "trip", r.PathValue("id"), map[string]any{"status": req.Status})
	w.WriteHeader(http.StatusNoContent)
}

func validTripStatus(status string) bool {
	switch status {
	case "planned", "assigned", "active", "completed", "cancelled":
		return true
	}
	return false
}
