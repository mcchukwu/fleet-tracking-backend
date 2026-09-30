// Package validator rejects malformed input before it enters the
// pipeline. This closes a real gap: nothing previously stopped lat=999
// or a zero-value timestamp from being written to Redis and evaluated
// against geofences, silently corrupting hot-path state.
package validator

import (
	"time"

	"github.com/mcchukwu/fleet-tracking-backend/internal/apperrors"
)

// MaxClockSkew tolerates minor client/server clock drift without
// rejecting a legitimate ping. It is NOT the out-of-order/lateness
// window this only rejects timestamps that are nonsensical (far future),
// not merely old ones, since old-but-valid timestamps are exactly what out-of-order
// handling exists to accept correctly rather than reject.
const MaxClockSkew = 5 * time.Minute

func ValidatePing(vehicleID string, lat, lon float64, recordedAt time.Time) error {
	if vehicleID == "" {
		return apperrors.Validation("vehicle_id is required")
	}
	if lat < -90 || lat > 90 {
		return apperrors.Validation("lat %f out of range [-90, 90]", lat)
	}
	if lon < -180 || lon > 180 {
		return apperrors.Validation("lon %f out of range [-180, 180]", lon)
	}
	if recordedAt.IsZero() {
		return apperrors.Validation("recorded_at is required")
	}
	if recordedAt.After(time.Now().Add(MaxClockSkew)) {
		return apperrors.Validation("recorded_at %s is too far in the future", recordedAt)
	}
	return nil
}
