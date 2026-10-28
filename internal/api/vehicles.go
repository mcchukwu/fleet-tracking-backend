package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type vehicle struct {
	ID          string  `json:"id"`
	ExternalID  string  `json:"external_id"`
	Plate       string  `json:"plate"`
	Type        string  `json:"vehicle_type"`
	Make        string  `json:"make"`
	Model       string  `json:"model"`
	Year        int     `json:"year,omitempty"`
	Colour      string  `json:"colour"`
	Status      string  `json:"status"`
	Depot       string  `json:"depot"`
	Department  string  `json:"department"`
	DriverID    string  `json:"driver_id,omitempty"`
	DriverName  string  `json:"driver_name,omitempty"`
	DriverPhone string  `json:"driver_phone,omitempty"`
	TripID      string  `json:"trip_id,omitempty"`
	Destination string  `json:"destination,omitempty"`
	TripStatus  string  `json:"trip_status,omitempty"`
	LastSeenAt  *string `json:"last_seen_at,omitempty"`
	Lat         float64 `json:"lat,omitempty"`
	Lon         float64 `json:"lon,omitempty"`
	SpeedKPH    float64 `json:"speed_kph,omitempty"`
	HeadingDeg  float64 `json:"heading_deg,omitempty"`
	RecordedAt  string  `json:"recorded_at,omitempty"`
	Online      bool    `json:"online"`
}

const vehicleSelect = `
	SELECT v.id, v.external_id, COALESCE(v.plate, ''), COALESCE(v.vehicle_type, ''),
	       COALESCE(v.make, ''), COALESCE(v.model, ''), COALESCE(v.year, 0),
	       COALESCE(v.colour, ''), v.status, COALESCE(v.depot, ''), COALESCE(v.department, ''),
	       d.id, d.name, d.phone,
	       t.id, t.destination, t.status,
	       v.last_seen_at
	FROM vehicles v
	LEFT JOIN drivers d ON d.id = v.current_driver_id
	LEFT JOIN LATERAL (
		SELECT id, destination, status
		FROM trips
		WHERE vehicle_id = v.id AND status IN ('assigned', 'active')
		ORDER BY created_at DESC LIMIT 1
	) t ON true
	WHERE v.tenant_id = $1 AND v.disabled_at IS NULL
`

func (s *Server) queryVehicles(ctx context.Context, tenantID, status, q string, limit, offset int) ([]vehicle, error) {
	rows, err := s.db.QueryContext(ctx, vehicleSelect+`
		AND ($2 = '' OR v.status = $2)
		AND ($3 = '' OR v.plate ILIKE '%' || $3 || '%' OR v.external_id ILIKE '%' || $3 || '%')
		ORDER BY v.plate NULLS LAST, v.external_id
		LIMIT $4 OFFSET $5
	`, tenantID, status, q, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []vehicle{}
	for rows.Next() {
		var v vehicle
		var year int
		var driverID, driverName, driverPhone, tripID, destination, tripStatus sql.NullString
		var lastSeen sql.NullTime
		if err := rows.Scan(
			&v.ID, &v.ExternalID, &v.Plate, &v.Type, &v.Make, &v.Model, &year,
			&v.Colour, &v.Status, &v.Depot, &v.Department,
			&driverID, &driverName, &driverPhone,
			&tripID, &destination, &tripStatus, &lastSeen,
		); err != nil {
			return nil, err
		}
		v.Year = year
		v.DriverID, v.DriverName, v.DriverPhone = driverID.String, driverName.String, driverPhone.String
		v.TripID, v.Destination, v.TripStatus = tripID.String, destination.String, tripStatus.String
		if lastSeen.Valid {
			t := lastSeen.Time.UTC().Format(time.RFC3339)
			v.LastSeenAt = &t
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Server) attachState(ctx context.Context, tenantID string, vehicles []vehicle) {
	if len(vehicles) == 0 {
		return
	}
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(vehicles))
	for i, v := range vehicles {
		cmds[i] = pipe.HGetAll(ctx, stateKey(tenantID, v.ExternalID))
	}
	_, _ = pipe.Exec(ctx)

	now := time.Now()
	for i := range vehicles {
		state, err := cmds[i].Result()
		if err != nil || len(state) == 0 {
			continue
		}
		vehicles[i].Lat, _ = strconv.ParseFloat(state["lat"], 64)
		vehicles[i].Lon, _ = strconv.ParseFloat(state["lon"], 64)
		vehicles[i].SpeedKPH, _ = strconv.ParseFloat(state["speed_kph"], 64)
		vehicles[i].HeadingDeg, _ = strconv.ParseFloat(state["heading_deg"], 64)
		vehicles[i].RecordedAt = state["recorded_at"]
		if t, err := time.Parse(time.RFC3339Nano, state["recorded_at"]); err == nil {
			vehicles[i].Online = now.Sub(t) < 5*time.Minute
		}
	}
}

func stateKey(tenantID, externalID string) string {
	return "tenant:" + tenantID + ":vehicle:" + externalID + ":state"
}

func (s *Server) listVehicles(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	limit, offset := s.page(r)
	vehicles, err := s.queryVehicles(r.Context(), user.TenantID,
		r.URL.Query().Get("status"), strings.TrimSpace(r.URL.Query().Get("q")), limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not list vehicles")
		return
	}
	s.attachState(r.Context(), user.TenantID, vehicles)
	writeJSON(w, http.StatusOK, map[string]any{"data": vehicles})
}

func (s *Server) getVehicle(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	var v vehicle
	var year int
	var driverID, driverName, driverPhone, tripID, destination, tripStatus sql.NullString
	var lastSeen sql.NullTime
	err := s.db.QueryRowContext(r.Context(), vehicleSelect+` AND v.id = $2`, user.TenantID, r.PathValue("id")).Scan(
		&v.ID, &v.ExternalID, &v.Plate, &v.Type, &v.Make, &v.Model, &year,
		&v.Colour, &v.Status, &v.Depot, &v.Department,
		&driverID, &driverName, &driverPhone,
		&tripID, &destination, &tripStatus, &lastSeen,
	)
	if err == sql.ErrNoRows {
		writeError(w, http.StatusNotFound, "not_found", "vehicle not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not load vehicle")
		return
	}
	v.Year = year
	v.DriverID, v.DriverName, v.DriverPhone = driverID.String, driverName.String, driverPhone.String
	v.TripID, v.Destination, v.TripStatus = tripID.String, destination.String, tripStatus.String
	if lastSeen.Valid {
		t := lastSeen.Time.UTC().Format(time.RFC3339)
		v.LastSeenAt = &t
	}
	vehicles := []vehicle{v}
	s.attachState(r.Context(), user.TenantID, vehicles)
	writeJSON(w, http.StatusOK, map[string]any{"data": vehicles[0]})
}

func (s *Server) vehicleHistory(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	to := time.Now().UTC()
	from := to.Add(-2 * time.Hour)
	if v := r.URL.Query().Get("to"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			to = t
		}
	}
	if v := r.URL.Query().Get("from"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			from = t
		}
	}
	if from.After(to) || to.Sub(from) > 7*24*time.Hour {
		writeError(w, http.StatusBadRequest, "invalid_range", "range must be positive and at most 7 days")
		return
	}

	resolution := r.URL.Query().Get("resolution")
	if resolution == "" {
		switch {
		case to.Sub(from) <= 6*time.Hour:
			resolution = "raw"
		case to.Sub(from) <= 48*time.Hour:
			resolution = "30s"
		default:
			resolution = "5m"
		}
	}

	type point struct {
		T    string  `json:"t"`
		Lat  float64 `json:"lat"`
		Lon  float64 `json:"lon"`
		Spd  float64 `json:"speed_kph"`
		Head float64 `json:"heading_deg"`
	}
	var points []point

	if resolution == "raw" {
		rows, err := s.db.QueryContext(r.Context(), `
			SELECT recorded_at, ST_Y(location::geometry), ST_X(location::geometry),
			       COALESCE(speed_kph, 0), COALESCE(heading_deg, 0)
			FROM position_history
			WHERE tenant_id = $1 AND vehicle_id = $2 AND recorded_at BETWEEN $3 AND $4
			ORDER BY recorded_at
			LIMIT 10000
		`, user.TenantID, r.PathValue("id"), from, to)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error", "could not load history")
			return
		}
		defer rows.Close()
		for rows.Next() {
			var p point
			var t time.Time
			if err := rows.Scan(&t, &p.Lat, &p.Lon, &p.Spd, &p.Head); err != nil {
				writeError(w, http.StatusInternalServerError, "internal_error", "could not read history")
				return
			}
			p.T = t.UTC().Format(time.RFC3339)
			points = append(points, p)
		}
	} else {
		interval := "30 seconds"
		if resolution == "5m" {
			interval = "5 minutes"
		}
		rows, err := s.db.QueryContext(r.Context(), `
			SELECT date_bin($5::interval, recorded_at, timestamptz '2000-01-01'),
			       avg(ST_Y(location::geometry)), avg(ST_X(location::geometry)),
			       coalesce(avg(speed_kph), 0), coalesce(avg(heading_deg), 0)
			FROM position_history
			WHERE tenant_id = $1 AND vehicle_id = $2 AND recorded_at BETWEEN $3 AND $4
			GROUP BY 1 ORDER BY 1
		`, user.TenantID, r.PathValue("id"), from, to, interval)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error", "could not load history")
			return
		}
		defer rows.Close()
		for rows.Next() {
			var p point
			var t time.Time
			if err := rows.Scan(&t, &p.Lat, &p.Lon, &p.Spd, &p.Head); err != nil {
				writeError(w, http.StatusInternalServerError, "internal_error", "could not read history")
				return
			}
			p.T = t.UTC().Format(time.RFC3339)
			points = append(points, p)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": points, "resolution": resolution})
}

func (s *Server) liveSnapshot(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	vehicles, err := s.queryVehicles(r.Context(), user.TenantID, "", "", 5000, 0)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not load vehicles")
		return
	}
	s.attachState(r.Context(), user.TenantID, vehicles)
	writeJSON(w, http.StatusOK, map[string]any{"data": vehicles})
}

func (s *Server) listGeofences(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	rows, err := s.db.QueryContext(r.Context(), `
		SELECT id, name, kind::text, ST_AsGeoJSON(boundary)
		FROM geofences WHERE tenant_id = $1 ORDER BY name
	`, user.TenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not load geofences")
		return
	}
	defer rows.Close()

	type geofence struct {
		ID       string          `json:"id"`
		Name     string          `json:"name"`
		Kind     string          `json:"kind"`
		Geometry json.RawMessage `json:"geometry"`
	}
	out := []geofence{}
	for rows.Next() {
		var g geofence
		var raw string
		if err := rows.Scan(&g.ID, &g.Name, &g.Kind, &raw); err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error", "could not read geofences")
			return
		}
		g.Geometry = json.RawMessage(raw)
		out = append(out, g)
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": out})
}

func (s *Server) systemHealth(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	health := map[string]any{"postgres": true, "redis": true}
	if err := s.db.PingContext(ctx); err != nil {
		health["postgres"] = false
	}
	if err := s.rdb.Ping(ctx).Err(); err != nil {
		health["redis"] = false
	}

	var vehicles, online, activeTrips, openAlerts int
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM vehicles WHERE tenant_id=$1 AND disabled_at IS NULL`, user.TenantID).Scan(&vehicles)
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM trips WHERE tenant_id=$1 AND status='active'`, user.TenantID).Scan(&activeTrips)
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM alerts WHERE tenant_id=$1 AND status='open'`, user.TenantID).Scan(&openAlerts)

	rows, err := s.db.QueryContext(ctx, `SELECT external_id FROM vehicles WHERE tenant_id=$1 AND disabled_at IS NULL LIMIT 5000`, user.TenantID)
	if err == nil {
		now := time.Now()
		for rows.Next() {
			var externalID string
			if rows.Scan(&externalID) != nil {
				continue
			}
			state, err := s.rdb.HGetAll(ctx, stateKey(user.TenantID, externalID)).Result()
			if err != nil {
				continue
			}
			if t, err := time.Parse(time.RFC3339Nano, state["recorded_at"]); err == nil && now.Sub(t) < 5*time.Minute {
				online++
			}
		}
		rows.Close()
	}

	var ingest any
	if raw, err := s.rdb.Get(ctx, "ftb:metrics:ingest").Result(); err == nil {
		ingest = json.RawMessage(raw)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"dependencies": health,
		"vehicles":     vehicles,
		"online":       online,
		"active_trips": activeTrips,
		"open_alerts":  openAlerts,
		"ingest":       ingest,
	})
}

type vehicleUpdate struct {
	Plate       *string `json:"plate"`
	DriverID    *string `json:"driver_id"`
	Status      *string `json:"status"`
	VehicleType *string `json:"vehicle_type"`
	Make        *string `json:"make"`
	Model       *string `json:"model"`
	Year        *int    `json:"year"`
	Colour      *string `json:"colour"`
	Depot       *string `json:"depot"`
	Department  *string `json:"department"`
	SimNumber   *string `json:"sim_number"`
	DeviceModel *string `json:"device_model"`
	Protocol    *string `json:"protocol"`
}

func (s *Server) updateVehicle(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	var req vehicleUpdate
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}

	sets := []string{}
	args := []any{r.PathValue("id"), user.TenantID}
	add := func(expr string, value any) {
		args = append(args, value)
		sets = append(sets, fmt.Sprintf(expr, len(args)))
	}
	if req.Plate != nil {
		add("plate = NULLIF($%d, '')", *req.Plate)
	}
	if req.DriverID != nil {
		add("current_driver_id = NULLIF($%d, '')::uuid", *req.DriverID)
	}
	if req.Status != nil {
		switch *req.Status {
		case "active", "maintenance", "inactive":
		default:
			writeError(w, http.StatusBadRequest, "invalid_status", "status must be active, maintenance or inactive")
			return
		}
		add("status = $%d", *req.Status)
	}
	if req.VehicleType != nil {
		add("vehicle_type = NULLIF($%d, '')", *req.VehicleType)
	}
	if req.Make != nil {
		add("make = NULLIF($%d, '')", *req.Make)
	}
	if req.Model != nil {
		add("model = NULLIF($%d, '')", *req.Model)
	}
	if req.Year != nil {
		add("year = NULLIF($%d, 0)", *req.Year)
	}
	if req.Colour != nil {
		add("colour = NULLIF($%d, '')", *req.Colour)
	}
	if req.Depot != nil {
		add("depot = NULLIF($%d, '')", *req.Depot)
	}
	if req.Department != nil {
		add("department = NULLIF($%d, '')", *req.Department)
	}
	if req.SimNumber != nil {
		add("sim_number = NULLIF($%d, '')", *req.SimNumber)
	}
	if req.DeviceModel != nil {
		add("device_model = NULLIF($%d, '')", *req.DeviceModel)
	}
	if req.Protocol != nil {
		add("protocol = NULLIF($%d, '')", *req.Protocol)
	}
	if len(sets) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "no fields to update")
		return
	}

	query := "UPDATE vehicles SET " + strings.Join(sets, ", ") + " WHERE id = $1 AND tenant_id = $2"
	tag, err := s.db.ExecContext(r.Context(), query, args...)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_update", "could not update vehicle")
		return
	}
	if n, _ := tag.RowsAffected(); n == 0 {
		writeError(w, http.StatusNotFound, "not_found", "vehicle not found")
		return
	}
	s.audit(r, user.TenantID, user.ID, "vehicle.update", "vehicle", r.PathValue("id"), nil)
	w.WriteHeader(http.StatusNoContent)
}

type vehicleEvent struct {
	Kind     string          `json:"kind"`
	Type     string          `json:"type"`
	Name     string          `json:"name,omitempty"`
	Severity string          `json:"severity,omitempty"`
	Status   string          `json:"status,omitempty"`
	At       string          `json:"at"`
	Evidence json.RawMessage `json:"evidence,omitempty"`
}

func (s *Server) vehicleEvents(w http.ResponseWriter, r *http.Request) {
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

	var exists bool
	if err := s.db.QueryRowContext(r.Context(),
		`SELECT EXISTS(SELECT 1 FROM vehicles WHERE id = $1 AND tenant_id = $2)`,
		r.PathValue("id"), user.TenantID).Scan(&exists); err != nil || !exists {
		writeError(w, http.StatusNotFound, "not_found", "vehicle not found")
		return
	}

	out := []vehicleEvent{}
	rows, err := s.db.QueryContext(r.Context(), `
		SELECT e.event_type::text, g.name, e.event_time
		FROM geofence_events e
		JOIN geofences g ON g.id = e.geofence_id
		WHERE e.tenant_id = $1 AND e.vehicle_id = $2 AND e.event_time BETWEEN $3 AND $4
		ORDER BY e.event_time DESC LIMIT 500
	`, user.TenantID, r.PathValue("id"), from, to)
	if err == nil {
		for rows.Next() {
			var ev vehicleEvent
			var at time.Time
			if rows.Scan(&ev.Type, &ev.Name, &at) == nil {
				ev.Kind = "geofence"
				ev.At = at.UTC().Format(time.RFC3339)
				out = append(out, ev)
			}
		}
		rows.Close()
	}

	rows, err = s.db.QueryContext(r.Context(), `
		SELECT type::text, severity::text, status::text, occurred_at, evidence
		FROM alerts
		WHERE tenant_id = $1 AND vehicle_id = $2 AND occurred_at BETWEEN $3 AND $4
		ORDER BY occurred_at DESC LIMIT 500
	`, user.TenantID, r.PathValue("id"), from, to)
	if err == nil {
		for rows.Next() {
			var ev vehicleEvent
			var at time.Time
			var evidence []byte
			if rows.Scan(&ev.Type, &ev.Severity, &ev.Status, &at, &evidence) == nil {
				ev.Kind = "alert"
				ev.At = at.UTC().Format(time.RFC3339)
				ev.Evidence = json.RawMessage(evidence)
				out = append(out, ev)
			}
		}
		rows.Close()
	}

	sort.Slice(out, func(i, j int) bool { return out[i].At > out[j].At })
	writeJSON(w, http.StatusOK, map[string]any{"data": out})
}
