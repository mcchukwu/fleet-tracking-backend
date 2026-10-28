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

func evaluateRules(ctx context.Context, rdb *redis.Client, pg *sql.DB, log *slog.Logger) {
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

	now := time.Now()
	for _, trip := range trips {
		state, err := rdb.HGetAll(ctx, stateKey(trip.tenantID, trip.externalID)).Result()
		if err != nil || len(state) == 0 {
			continue
		}
		recorded, _ := time.Parse(time.RFC3339Nano, state["recorded_at"])
		speed, _ := parseFloat(state["speed_kph"])

		if !recorded.IsZero() && now.Sub(recorded) > 10*time.Minute {
			raiseAlert(ctx, pg, trip, "device_offline", "high", now, map[string]any{
				"last_seen": recorded.UTC().Format(time.RFC3339),
			})
		}
		if speed > 80 {
			raiseAlert(ctx, pg, trip, "speeding", "medium", recorded, map[string]any{"speed_kph": speed})
		}

		if trip.hasRoute {
			var distance float64
			err := pg.QueryRowContext(ctx, `
				SELECT ST_Distance(p.location, t.planned_route)
				FROM position_history p
				JOIN trips t ON t.id = $1
				WHERE p.vehicle_id = t.vehicle_id AND p.recorded_at > now() - interval '5 minutes'
				ORDER BY p.recorded_at DESC LIMIT 1
			`, trip.id).Scan(&distance)
			if err == nil && distance > 2000 {
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
		if err == nil && count >= 3 && maxSpeed < 2 && now.Sub(minTime) > 10*time.Minute && now.Sub(maxTime) < 5*time.Minute {
			raiseAlert(ctx, pg, trip, "idle", "medium", now, map[string]any{
				"minutes": int(now.Sub(minTime).Minutes()),
			})
		}
	}
}

func raiseAlert(ctx context.Context, pg *sql.DB, trip activeTrip, kind, severity string, at time.Time, evidence map[string]any) {
	if at.IsZero() {
		at = time.Now()
	}
	payload, _ := json.Marshal(evidence)
	_, err := pg.ExecContext(ctx, `
		INSERT INTO alerts (tenant_id, vehicle_id, trip_id, type, severity, occurred_at, evidence)
		VALUES ($1, $2, $3, $4::alert_type, $5::alert_severity, $6, $7::jsonb)
		ON CONFLICT DO NOTHING
	`, trip.tenantID, trip.vehicleUUID, trip.id, kind, severity, at, string(payload))
	if err != nil {
		slog.Default().Error("alert insert failed", "type", kind, "vehicle", trip.externalID, "error", err)
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
