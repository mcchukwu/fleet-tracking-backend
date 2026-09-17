package alert

import "time"

type AlertEvent struct {
	VehicleID    string        `json:"vehicle_id"`
	GeofenceID   string        `json:"geofence_id"`
	GeofenceName string        `json:"geofence_name"`
	EventType    string        `json:"event_type"` // "enter" | "exit"
	EventTime    time.Time     `json:"event_time"`
	Latency      time.Duration `json:"latency_ns"`
}
