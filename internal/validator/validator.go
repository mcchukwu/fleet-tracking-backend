// Package validator rejects malformed frames before they reach Redis.
package validator

import (
	"fmt"
	"math"
	"time"
)

// MaxClockSkew is the future tolerance. Old timestamps are accepted down
// to MaxAge because late delivery is normal.
const (
	MaxClockSkew = 30 * time.Second
	MaxAge       = 30 * 24 * time.Hour
)

func ValidatePing(vehicleID string, lat, lon float64, recordedAt time.Time) error {
	if vehicleID == "" {
		return fmt.Errorf("vehicle_id is required")
	}
	if math.IsNaN(lat) || math.IsInf(lat, 0) {
		return fmt.Errorf("lat must be finite")
	}
	if math.IsNaN(lon) || math.IsInf(lon, 0) {
		return fmt.Errorf("lon must be finite")
	}
	if lat < -90 || lat > 90 {
		return fmt.Errorf("lat %f out of range [-90, 90]", lat)
	}
	if lon < -180 || lon > 180 {
		return fmt.Errorf("lon %f out of range [-180, 180]", lon)
	}
	if recordedAt.IsZero() {
		return fmt.Errorf("recorded_at is required")
	}
	if recordedAt.After(time.Now().Add(MaxClockSkew)) {
		return fmt.Errorf("recorded_at %s is too far in the future", recordedAt)
	}
	if recordedAt.Before(time.Now().Add(-MaxAge)) {
		return fmt.Errorf("recorded_at %s is too old", recordedAt)
	}
	return nil
}

func ValidateMotion(speedKPH, headingDeg float64) error {
	if math.IsNaN(speedKPH) || math.IsInf(speedKPH, 0) || speedKPH < 0 || speedKPH > 300 {
		return fmt.Errorf("speed_kph %f out of range [0, 300]", speedKPH)
	}
	if math.IsNaN(headingDeg) || math.IsInf(headingDeg, 0) || headingDeg < 0 || headingDeg > 360 {
		return fmt.Errorf("heading_deg %f out of range [0, 360]", headingDeg)
	}
	return nil
}
