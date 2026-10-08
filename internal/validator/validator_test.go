package validator

import (
	"math"
	"testing"
	"time"
)

func TestValidatePing(t *testing.T) {
	now := time.Now()
	for _, test := range []struct {
		name string
		id   string
		lat  float64
		lon  float64
		at   time.Time
		want bool
	}{
		{"valid", "vehicle-1", 6.5, 3.4, now, false},
		{"missing ID", "", 6.5, 3.4, now, true},
		{"latitude out of range", "vehicle-1", 91, 3.4, now, true},
		{"longitude out of range", "vehicle-1", 6.5, 181, now, true},
		{"NaN latitude", "vehicle-1", math.NaN(), 3.4, now, true},
		{"infinite longitude", "vehicle-1", 6.5, math.Inf(1), now, true},
		{"missing timestamp", "vehicle-1", 6.5, 3.4, time.Time{}, true},
		{"far-future timestamp", "vehicle-1", 6.5, 3.4, now.Add(MaxClockSkew + time.Second), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			gotErr := ValidatePing(test.id, test.lat, test.lon, test.at) != nil
			if gotErr != test.want {
				t.Fatalf("ValidatePing error = %v, want %v", gotErr, test.want)
			}
		})
	}
}
