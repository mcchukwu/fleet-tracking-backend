package api

import (
	"encoding/json"
	"net/http"
	"strings"
)

type geofenceRequest struct {
	Name     string          `json:"name"`
	Kind     string          `json:"kind"`
	Geometry json.RawMessage `json:"geometry"`
}

func validGeofenceKind(kind string) bool {
	switch kind {
	case "", "depot", "customer", "restricted", "other":
		return true
	}
	return false
}

func (s *Server) createGeofence(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	var req geofenceRequest
	if err := decode(r, &req); err != nil || strings.TrimSpace(req.Name) == "" || len(req.Geometry) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "name and geometry are required")
		return
	}
	if !validGeofenceKind(req.Kind) {
		writeError(w, http.StatusBadRequest, "invalid_kind", "kind must be depot, customer, restricted or other")
		return
	}
	if req.Kind == "" {
		req.Kind = "other"
	}

	var id string
	err := s.db.QueryRowContext(r.Context(), `
		INSERT INTO geofences (tenant_id, name, boundary, kind)
		SELECT $1, $2, ST_SetSRID(ST_GeomFromGeoJSON($3), 4326)::geography, $4::geofence_kind
		WHERE ST_IsValid(ST_SetSRID(ST_GeomFromGeoJSON($3), 4326))
		  AND ST_GeometryType(ST_SetSRID(ST_GeomFromGeoJSON($3), 4326)) = 'ST_Polygon'
		RETURNING id
	`, user.TenantID, req.Name, string(req.Geometry), req.Kind).Scan(&id)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_geometry", "geometry must be a valid polygon")
		return
	}
	s.bumpGeofences(r)
	s.audit(r, user.TenantID, user.ID, "geofence.create", "geofence", id, nil)
	writeJSON(w, http.StatusCreated, map[string]any{"id": id})
}

func (s *Server) updateGeofence(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	var req geofenceRequest
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	if strings.TrimSpace(req.Name) == "" && len(req.Geometry) == 0 && req.Kind == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "name, geometry or kind is required")
		return
	}
	if !validGeofenceKind(req.Kind) {
		writeError(w, http.StatusBadRequest, "invalid_kind", "kind must be depot, customer, restricted or other")
		return
	}

	var query string
	var args []any
	if len(req.Geometry) > 0 {
		query = `
			UPDATE geofences SET name = COALESCE(NULLIF($3, ''), name),
				boundary = ST_SetSRID(ST_GeomFromGeoJSON($4), 4326)::geography,
				kind = COALESCE(NULLIF($5, '')::geofence_kind, kind)
			WHERE id = $1 AND tenant_id = $2
			  AND ST_IsValid(ST_SetSRID(ST_GeomFromGeoJSON($4), 4326))
			  AND ST_GeometryType(ST_SetSRID(ST_GeomFromGeoJSON($4), 4326)) = 'ST_Polygon'
		`
		args = []any{r.PathValue("id"), user.TenantID, req.Name, string(req.Geometry), req.Kind}
	} else {
		query = `UPDATE geofences SET name = COALESCE(NULLIF($3, ''), name),
			kind = COALESCE(NULLIF($4, '')::geofence_kind, kind)
			WHERE id = $1 AND tenant_id = $2`
		args = []any{r.PathValue("id"), user.TenantID, req.Name, req.Kind}
	}
	tag, err := s.db.ExecContext(r.Context(), query, args...)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_geometry", "could not update geofence")
		return
	}
	if n, _ := tag.RowsAffected(); n == 0 {
		writeError(w, http.StatusNotFound, "not_found", "geofence not found")
		return
	}
	s.bumpGeofences(r)
	s.audit(r, user.TenantID, user.ID, "geofence.update", "geofence", r.PathValue("id"), nil)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) deleteGeofence(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	tag, err := s.db.ExecContext(r.Context(),
		`DELETE FROM geofences WHERE id = $1 AND tenant_id = $2`,
		r.PathValue("id"), user.TenantID)
	if err != nil {
		writeError(w, http.StatusConflict, "in_use", "geofence has events and cannot be deleted")
		return
	}
	if n, _ := tag.RowsAffected(); n == 0 {
		writeError(w, http.StatusNotFound, "not_found", "geofence not found")
		return
	}
	s.bumpGeofences(r)
	s.audit(r, user.TenantID, user.ID, "geofence.delete", "geofence", r.PathValue("id"), nil)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) bumpGeofences(r *http.Request) {
	_, _ = s.rdb.Incr(r.Context(), "ftb:geofence_version").Result()
}
