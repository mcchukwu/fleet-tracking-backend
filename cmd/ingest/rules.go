package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

type activeTrip struct {
	id          string
	tenantID    string
	vehicleUUID string
	externalID  string
	hasRoute    bool
}

func runAlertRules(rdb *redis.Client, pg *sql.DB, log *slog.Logger) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		evaluateRules(ctx, rdb, pg, log)
		cancel()
	}
}

type rule struct {
	enabled  bool
	speed    float64
	duration float64
	distance float64
}

func loadRules(ctx context.Context, pg *sql.DB, tenantID string) map[string]rule {
	rules := map[string]rule{}
	rows, err := pg.QueryContext(ctx, `
		SELECT type::text, enabled, config FROM alert_rules WHERE tenant_id = $1
	`, tenantID)
	if err != nil {
		return rules
	}
	defer rows.Close()
	if rows.Err() != nil {
		return rules
	}
	for rows.Next() {
		var kind string
		var enabled bool
		var raw []byte
		if rows.Scan(&kind, &enabled, &raw) != nil {
			continue
		}
		var cfg map[string]float64
		_ = json.Unmarshal(raw, &cfg)
		r := rule{enabled: enabled, speed: cfg["speed_kph"], duration: cfg["duration_s"], distance: cfg["distance_m"]}
		rules[kind] = r
	}
	return rules
}

func (r rule) or(defaults rule) rule {
	if r.speed == 0 {
		r.speed = defaults.speed
	}
	if r.duration == 0 {
		r.duration = defaults.duration
	}
	if r.distance == 0 {
		r.distance = defaults.distance
	}
	return r
}

func evaluateRules(ctx context.Context, rdb *redis.Client, pg *sql.DB, log *slog.Logger) {
	unplannedTripAlerts(ctx, pg, log)
	rows, err := pg.QueryContext(ctx, `
		SELECT t.id, t.tenant_id, t.vehicle_id, v.external_id, t.planned_route IS NOT NULL
		FROM trips t
		JOIN vehicles v ON v.id = t.vehicle_id
		WHERE t.status = 'active' AND v.disabled_at IS NULL
	`)
	if err != nil {
		log.Error("alert rules: active trips query failed", "error", err)
		return
	}
	var trips []activeTrip
	for rows.Next() {
		var t activeTrip
		if rows.Scan(&t.id, &t.tenantID, &t.vehicleUUID, &t.externalID, &t.hasRoute) == nil {
			trips = append(trips, t)
		}
	}
	rows.Close()
	if rows.Err() != nil {
		log.Error("alert rules: active trips query failed", "error", err)
		return
	}

	rulesByTenant := map[string]map[string]rule{}
	now := time.Now()
	for _, trip := range trips {
		rules, ok := rulesByTenant[trip.tenantID]
		if !ok {
			rules = loadRules(ctx, pg, trip.tenantID)
			rulesByTenant[trip.tenantID] = rules
		}
		offline := rule{enabled: true, duration: 600}
		if r, ok := rules["device_offline"]; ok {
			offline = r.or(offline)
		}
		speeding := rule{enabled: true, speed: 80}
		if r, ok := rules["speeding"]; ok {
			speeding = r.or(speeding)
		}
		deviation := rule{enabled: true, distance: 2000}
		if r, ok := rules["route_deviation"]; ok {
			deviation = r.or(deviation)
		}
		idle := rule{enabled: true, duration: 600, speed: 2}
		if r, ok := rules["idle"]; ok {
			idle = r.or(idle)
		}
		state, err := rdb.HGetAll(ctx, stateKey(trip.tenantID, trip.externalID)).Result()
		if err != nil || len(state) == 0 {
			continue
		}
		recorded, _ := time.Parse(time.RFC3339Nano, state["recorded_at"])
		speed, _ := parseFloat(state["speed_kph"])

		if offline.enabled && !recorded.IsZero() && now.Sub(recorded) > time.Duration(offline.duration)*time.Second {
			raiseAlert(ctx, pg, trip, "device_offline", "high", now, map[string]any{
				"last_seen": recorded.UTC().Format(time.RFC3339),
			})
		}
		if speeding.enabled && speed > speeding.speed {
			raiseAlert(ctx, pg, trip, "speeding", "medium", recorded, map[string]any{"speed_kph": speed})
		}

		if trip.hasRoute && deviation.enabled {
			var distance float64
			err := pg.QueryRowContext(ctx, `
				SELECT ST_Distance(p.location, t.planned_route)
				FROM position_history p
				JOIN trips t ON t.id = $1
				WHERE p.vehicle_id = t.vehicle_id AND p.recorded_at > now() - interval '5 minutes'
				ORDER BY p.recorded_at DESC LIMIT 1
			`, trip.id).Scan(&distance)
			if err == nil && distance > deviation.distance {
				raiseAlert(ctx, pg, trip, "route_deviation", "high", now, map[string]any{
					"distance_m": distance,
				})
			}
		}

		var count int
		var minTime, maxTime time.Time
		var maxSpeed float64
		err = pg.QueryRowContext(ctx, `
			SELECT count(*), min(recorded_at), max(recorded_at), coalesce(max(speed_kph), 0)
			FROM position_history
			WHERE vehicle_id = $1 AND recorded_at > now() - interval '15 minutes'
		`, trip.vehicleUUID).Scan(&count, &minTime, &maxTime, &maxSpeed)
		if idle.enabled && err == nil && count >= 3 && maxSpeed < idle.speed &&
			now.Sub(minTime) > time.Duration(idle.duration)*time.Second && now.Sub(maxTime) < 5*time.Minute {
			raiseAlert(ctx, pg, trip, "idle", "medium", now, map[string]any{
				"minutes": int(now.Sub(minTime).Minutes()),
			})
		}
	}
}

func raiseAlert(ctx context.Context, pg *sql.DB, trip activeTrip, kind, severity string, at time.Time, evidence map[string]any) {
	insertAlert(ctx, pg, trip.tenantID, trip.vehicleUUID, trip.id, trip.externalID, kind, severity, at, evidence)
}

func insertAlert(ctx context.Context, pg *sql.DB, tenantID, vehicleUUID, tripID, externalID, kind, severity string, at time.Time, evidence map[string]any) {
	if at.IsZero() {
		at = time.Now()
	}
	payload, _ := json.Marshal(evidence)
	_, err := pg.ExecContext(ctx, `
		INSERT INTO alerts (tenant_id, vehicle_id, trip_id, type, severity, occurred_at, evidence)
		VALUES ($1, $2, NULLIF($3, '')::uuid, $4::alert_type, $5::alert_severity, $6, $7::jsonb)
		ON CONFLICT DO NOTHING
	`, tenantID, vehicleUUID, tripID, kind, severity, at, string(payload))
	if err != nil {
		slog.Default().Error("alert insert failed", "type", kind, "vehicle", externalID, "error", err)
	}
}

// unplannedTripAlerts looks for a depot exit with no assigned or active
// trip. That is the case dispatch usually misses.
func unplannedTripAlerts(ctx context.Context, pg *sql.DB, log *slog.Logger) {
	rows, err := pg.QueryContext(ctx, `
		SELECT e.tenant_id, e.vehicle_id, v.external_id, g.id, g.name, e.event_time
		FROM geofence_events e
		JOIN geofences g ON g.id = e.geofence_id
		JOIN vehicles v ON v.id = e.vehicle_id
		WHERE e.event_type = 'exit'
		  AND g.kind = 'depot'
		  AND e.event_time > now() - interval '2 minutes'
		  AND NOT EXISTS (
			  SELECT 1 FROM trips t
			  WHERE t.vehicle_id = e.vehicle_id AND t.status IN ('assigned', 'active')
		  )
	`)
	if err != nil {
		log.Error("unplanned trip query failed", "error", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var tenantID, vehicleID, externalID, geofenceID, name string
		var at time.Time
		if rows.Scan(&tenantID, &vehicleID, &externalID, &geofenceID, &name, &at) != nil {
			continue
		}
		rules := loadRules(ctx, pg, tenantID)
		if r, ok := rules["unplanned_trip"]; ok && !r.enabled {
			continue
		}
		insertAlert(ctx, pg, tenantID, vehicleID, "", externalID, "unplanned_trip", "high", at, map[string]any{
			"geofence_id": geofenceID, "geofence_name": name,
		})
	}
}

func stateKey(tenantID, externalID string) string {
	return "tenant:" + tenantID + ":vehicle:" + externalID + ":state"
}

func parseFloat(v string) (float64, bool) {
	var f float64
	if v == "" {
		return 0, false
	}
	for _, c := range v {
		if c == '.' || c == '-' || c == '+' || (c >= '0' && c <= '9') {
			continue
		}
		return 0, false
	}
	fmt.Sscanf(v, "%f", &f)
	return f, true
}
