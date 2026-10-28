package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/mcchukwu/fleet-tracking-backend/internal/config"
	"github.com/mcchukwu/fleet-tracking-backend/internal/db"
	"github.com/mcchukwu/fleet-tracking-backend/internal/identity"
)

const (
	demoTenantID = "00000000-0000-0000-0000-000000000010"
	demoPassword = "DemoPass123!"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg, err := config.LoadAPI()
	if err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	sqlDB, err := db.ConnectPostgres(cfg.PGDSN)
	if err != nil {
		log.Error("connect", "error", err)
		os.Exit(1)
	}
	defer sqlDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	tx, err := sqlDB.BeginTx(ctx, nil)
	if err != nil {
		log.Error("begin", "error", err)
		os.Exit(1)
	}
	defer tx.Rollback()

	steps := []struct {
		name string
		fn   func(context.Context, *sql.Tx) error
	}{
		{"tenant", seedTenant},
		{"users", seedUsers},
		{"fleet", seedFleet},
		{"geofences", seedGeofences},
		{"rules", seedRules},
		{"trips", seedTrips},
	}
	for _, step := range steps {
		if err := step.fn(ctx, tx); err != nil {
			log.Error("seed failed", "step", step.name, "error", err)
			os.Exit(1)
		}
	}
	if err := tx.Commit(); err != nil {
		log.Error("commit", "error", err)
		os.Exit(1)
	}
	fmt.Println("seeded Demo Fleet")
	fmt.Printf("login: dispatcher@demo.ftb / %s\n", demoPassword)
	fmt.Println("device key: demo-device-key")
}

func seedTenant(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO tenants (id, name) VALUES ($1, 'Demo Fleet')
		ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
	`, demoTenantID); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO api_keys (tenant_id, key_hash, label)
		VALUES ($1, $2, 'demo device key')
		ON CONFLICT (key_hash) DO NOTHING
	`, demoTenantID, identity.HashToken("demo-device-key"))
	return err
}

func seedUsers(ctx context.Context, tx *sql.Tx) error {
	hash, err := identity.HashPassword(demoPassword)
	if err != nil {
		return err
	}
	users := []struct {
		id, email, name, role string
	}{
		{"00000000-0000-0000-0000-000000000101", "admin@demo.ftb", "Demo Admin", "admin"},
		{"00000000-0000-0000-0000-000000000102", "dispatcher@demo.ftb", "Demo Dispatcher", "dispatcher"},
		{"00000000-0000-0000-0000-000000000103", "security@demo.ftb", "Demo Security", "security"},
		{"00000000-0000-0000-0000-000000000104", "viewer@demo.ftb", "Demo Viewer", "viewer"},
	}
	for _, u := range users {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO users (id, tenant_id, email, password_hash, name, role)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (id) DO UPDATE
			SET password_hash = EXCLUDED.password_hash, name = EXCLUDED.name, role = EXCLUDED.role, status = 'active'
		`, u.id, demoTenantID, u.email, hash, u.name, u.role); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM sessions WHERE user_id IN (SELECT id FROM users WHERE tenant_id = $1)`, demoTenantID)
	return err
}

func seedFleet(ctx context.Context, tx *sql.Tx) error {
	for _, q := range []string{
		`DELETE FROM alerts WHERE tenant_id = $1`,
		`DELETE FROM geofence_events WHERE tenant_id = $1`,
		`DELETE FROM trips WHERE tenant_id = $1`,
		`DELETE FROM assignments WHERE tenant_id = $1`,
		`DELETE FROM position_history WHERE tenant_id = $1`,
		`DELETE FROM vehicles WHERE tenant_id = $1`,
		`DELETE FROM drivers WHERE tenant_id = $1`,
	} {
		if _, err := tx.ExecContext(ctx, q, demoTenantID); err != nil {
			return err
		}
	}

	firstNames := []string{"Adebayo", "Chinedu", "Musa", "Tunde", "Emeka", "Sani", "Femi", "Ibrahim", "Kunle", "Uche"}
	lastNames := []string{"Okafor", "Bello", "Adeyemi", "Musa", "Eze", "Yusuf", "Balogun", "Ali", "Ojo", "Nwosu"}
	driverIDs := make([]string, 0, 20)
	for i := range 20 {
		var id string
		err := tx.QueryRowContext(ctx, `
			INSERT INTO drivers (tenant_id, name, phone, licence)
			VALUES ($1, $2, $3, $4) RETURNING id
		`, demoTenantID,
			firstNames[i%len(firstNames)]+" "+lastNames[(i*3)%len(lastNames)],
			fmt.Sprintf("0803%07d", 1000000+i),
			fmt.Sprintf("LIC-%05d", 1000+i),
		).Scan(&id)
		if err != nil {
			return err
		}
		driverIDs = append(driverIDs, id)
	}

	for i := range 20 {
		plate := fmt.Sprintf("LSD-%03d-AA", 100+i)
		externalID := fmt.Sprintf("demo-imei-%04d", i+1)
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO vehicles (
				tenant_id, external_id, plate, vehicle_type, make, model, year, colour,
				chassis_number, sim_number, device_model, protocol, depot, department, current_driver_id
			) VALUES ($1,$2,$3,'tanker','MAN','TGS',2021,'white',$4,$5,'QS109','tcp',$6,'operations',$7)
			ON CONFLICT (tenant_id, external_id) DO UPDATE SET
				plate = EXCLUDED.plate, vehicle_type = EXCLUDED.vehicle_type,
				make = EXCLUDED.make, model = EXCLUDED.model, year = EXCLUDED.year,
				colour = EXCLUDED.colour, current_driver_id = EXCLUDED.current_driver_id
		`, demoTenantID, externalID, plate,
			fmt.Sprintf("CHS%08d", 20000000+i),
			fmt.Sprintf("0809%07d", 2000000+i),
			[]string{"Lekki Depot", "Apapa Depot", "Ikeja Depot"}[i%3],
			driverIDs[i],
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO assignments (tenant_id, vehicle_id, driver_id)
			SELECT $1, id, $2 FROM vehicles WHERE tenant_id = $1 AND external_id = $3
		`, demoTenantID, driverIDs[i], externalID); err != nil {
			return err
		}
	}
	return nil
}

func seedGeofences(ctx context.Context, tx *sql.Tx) error {
	zones := []struct {
		id, name, kind, wkt string
	}{
		{"00000000-0000-0000-0000-000000000201", "Refinery Gate", "restricted", "SRID=4326;POLYGON((3.380 6.440, 3.400 6.440, 3.400 6.455, 3.380 6.455, 3.380 6.440))"},
		{"00000000-0000-0000-0000-000000000202", "Lekki Depot", "depot", "SRID=4326;POLYGON((3.470 6.430, 3.490 6.430, 3.490 6.445, 3.470 6.445, 3.470 6.430))"},
		{"00000000-0000-0000-0000-000000000203", "Ibadan Customer", "customer", "SRID=4326;POLYGON((3.870 7.360, 3.900 7.360, 3.900 7.385, 3.870 7.385, 3.870 7.360))"},
	}
	for _, z := range zones {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO geofences (id, tenant_id, name, boundary, kind)
			VALUES ($1, $2, $3, ST_GeogFromText($4), $5::geofence_kind)
			ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, boundary = EXCLUDED.boundary, kind = EXCLUDED.kind
		`, z.id, demoTenantID, z.name, z.wkt, z.kind); err != nil {
			return err
		}
	}
	return nil
}

func seedRules(ctx context.Context, tx *sql.Tx) error {
	rules := []struct {
		kind   string
		config string
	}{
		{"geofence_exit", `{}`},
		{"route_deviation", `{"distance_m":2000,"duration_s":180}`},
		{"idle", `{"speed_kph":2,"duration_s":600}`},
		{"speeding", `{"speed_kph":80,"duration_s":120}`},
		{"device_offline", `{"duration_s":600}`},
		{"unplanned_trip", `{}`},
	}
	for _, r := range rules {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO alert_rules (tenant_id, type, enabled, config)
			VALUES ($1, $2, true, $3::jsonb)
			ON CONFLICT (tenant_id, type) DO UPDATE SET config = EXCLUDED.config
		`, demoTenantID, r.kind, r.config); err != nil {
			return err
		}
	}
	return nil
}

func seedTrips(ctx context.Context, tx *sql.Tx) error {
	for i := 1; i <= 5; i++ {
		externalID := fmt.Sprintf("demo-imei-%04d", i)
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO trips (tenant_id, vehicle_id, driver_id, origin, destination, planned_route, status, started_at, created_by)
			SELECT $1, v.id, v.current_driver_id, 'Refinery Gate', 'Ibadan Customer',
			       ST_GeogFromText('SRID=4326;LINESTRING(3.390 6.447, 3.520 6.650, 3.700 7.050, 3.880 7.370)'),
			       'active', now() - interval '90 minutes', $2
			FROM vehicles v WHERE v.tenant_id = $1 AND v.external_id = $3
		`, demoTenantID, "00000000-0000-0000-0000-000000000102", externalID); err != nil {
			return err
		}
	}
	return nil
}
