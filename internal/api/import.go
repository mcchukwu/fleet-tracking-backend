package api

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type importError struct {
	Row     int    `json:"row"`
	Message string `json:"message"`
}

func (s *Server) importVehicles(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())
	if err := r.ParseMultipartForm(20 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "expected a multipart file field named file")
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "expected a multipart file field named file")
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	reader.TrimLeadingSpace = true

	headers, err := reader.Read()
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_csv", "could not read the header row")
		return
	}
	index := make(map[string]int, len(headers))
	for i, h := range headers {
		index[strings.ToLower(strings.TrimSpace(strings.TrimPrefix(h, "\ufeff")))] = i
	}
	if _, ok := index["imei"]; !ok {
		writeError(w, http.StatusBadRequest, "invalid_csv", "the header must include imei")
		return
	}

	var jobID string
	err = s.db.QueryRowContext(r.Context(), `
		INSERT INTO import_jobs (tenant_id, user_id, filename, status)
		VALUES ($1, $2, $3, 'running') RETURNING id
	`, user.TenantID, user.ID, header.Filename).Scan(&jobID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not start import")
		return
	}

	var imported, failed int
	var problems []importError
	rowNumber := 1
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		rowNumber++
		if err != nil {
			failed++
			if len(problems) < 100 {
				problems = append(problems, importError{Row: rowNumber, Message: "malformed row"})
			}
			continue
		}
		if err := s.importRow(r.Context(), user.TenantID, row, index); err != nil {
			failed++
			if len(problems) < 100 {
				problems = append(problems, importError{Row: rowNumber, Message: err.Error()})
			}
			continue
		}
		imported++
	}

	problemsJSON, _ := json.Marshal(problems)
	status := "completed"
	if failed > 0 && imported == 0 {
		status = "failed"
	}
	_, _ = s.db.ExecContext(r.Context(), `
		UPDATE import_jobs SET total = $2, imported = $3, failed = $4, status = $5,
			errors = $6::jsonb, finished_at = now()
		WHERE id = $1
	`, jobID, imported+failed, imported, failed, status, string(problemsJSON))

	s.audit(r, user.TenantID, user.ID, "import.vehicles", "import_job", jobID, map[string]any{"imported": imported, "failed": failed})
	writeJSON(w, http.StatusOK, map[string]any{
		"job_id": jobID, "total": imported + failed, "imported": imported, "failed": failed, "errors": problems,
	})
}

func field(row []string, index map[string]int, name string) string {
	i, ok := index[name]
	if !ok || i >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[i])
}

func (s *Server) importRow(ctx context.Context, tenantID string, row []string, index map[string]int) error {
	imei := field(row, index, "imei")
	if imei == "" {
		return fmt.Errorf("imei is required")
	}

	var year any
	if raw := field(row, index, "year"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return fmt.Errorf("year is not a number")
		}
		year = n
	}

	var vehicleID string
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO vehicles (
			tenant_id, external_id, plate, vehicle_type, make, model, year, colour,
			chassis_number, sim_number, device_model, protocol, depot, department
		) VALUES ($1,$2,NULLIF($3,''),NULLIF($4,''),NULLIF($5,''),NULLIF($6,''),$7,NULLIF($8,''),
		          NULLIF($9,''),NULLIF($10,''),NULLIF($11,''),NULLIF($12,''),NULLIF($13,''),NULLIF($14,''))
		ON CONFLICT (tenant_id, external_id) DO UPDATE SET
			plate = COALESCE(NULLIF(EXCLUDED.plate, ''), vehicles.plate),
			vehicle_type = COALESCE(NULLIF(EXCLUDED.vehicle_type, ''), vehicles.vehicle_type),
			make = COALESCE(NULLIF(EXCLUDED.make, ''), vehicles.make),
			model = COALESCE(NULLIF(EXCLUDED.model, ''), vehicles.model),
			year = COALESCE(EXCLUDED.year, vehicles.year),
			colour = COALESCE(NULLIF(EXCLUDED.colour, ''), vehicles.colour),
			chassis_number = COALESCE(NULLIF(EXCLUDED.chassis_number, ''), vehicles.chassis_number),
			sim_number = COALESCE(NULLIF(EXCLUDED.sim_number, ''), vehicles.sim_number),
			device_model = COALESCE(NULLIF(EXCLUDED.device_model, ''), vehicles.device_model),
			protocol = COALESCE(NULLIF(EXCLUDED.protocol, ''), vehicles.protocol),
			depot = COALESCE(NULLIF(EXCLUDED.depot, ''), vehicles.depot),
			department = COALESCE(NULLIF(EXCLUDED.department, ''), vehicles.department)
		RETURNING id
	`, tenantID, imei,
		field(row, index, "plate"), field(row, index, "vehicle_type"),
		field(row, index, "make"), field(row, index, "model"), year,
		field(row, index, "colour"), field(row, index, "chassis_number"),
		field(row, index, "sim_number"), field(row, index, "device_model"),
		field(row, index, "protocol"), field(row, index, "depot"),
		field(row, index, "department"),
	).Scan(&vehicleID)
	if err != nil {
		return fmt.Errorf("could not save vehicle: %w", err)
	}

	name := field(row, index, "driver_name")
	phone := field(row, index, "driver_phone")
	if name == "" && phone == "" {
		return nil
	}

	var driverID string
	err = s.db.QueryRowContext(ctx, `
		SELECT id FROM drivers
		WHERE tenant_id = $1 AND (($2 <> '' AND phone = $2) OR ($2 = '' AND name = $3))
		LIMIT 1
	`, tenantID, phone, name).Scan(&driverID)
	if err == sql.ErrNoRows {
		err = s.db.QueryRowContext(ctx, `
			INSERT INTO drivers (tenant_id, name, phone, licence)
			VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''))
			RETURNING id
		`, tenantID, firstNonEmpty(name, phone), phone, field(row, index, "driver_licence")).Scan(&driverID)
	}
	if err != nil {
		return fmt.Errorf("could not save driver")
	}

	_, err = s.db.ExecContext(ctx, `
		UPDATE vehicles SET current_driver_id = $3 WHERE id = $1 AND tenant_id = $2
	`, vehicleID, tenantID, driverID)
	if err != nil {
		return fmt.Errorf("could not assign driver")
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
