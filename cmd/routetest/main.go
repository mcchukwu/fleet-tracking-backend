// routetest sends one vehicle along a deliberate straight line path that
// starts outside the seeded "Test Yard" geofence, crosses through it, and
// exits the far side, then queries the server's /debug/alerts endpoint
// and checks that exactly one enter and one exit were recorded for it.
//
// This is the Phase 2 correctness check the stochastic load generator
// can't give you: with 5,000 randomly drifting vehicles you can't predict
// who crosses what, when. This tool trades scale for a known correct
// answer, which is the whole point of testing correctness and throughput
// as two separate concerns instead of hoping one test proves both.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type PositionPing struct {
	VehicleID  string    `json:"vehicle_id"`
	Lat        float64   `json:"lat"`
	Lon        float64   `json:"lon"`
	RecordedAt time.Time `json:"recorded_at"`
}

type AlertEvent struct {
	VehicleID    string  `json:"vehicle_id"`
	GeofenceID   string  `json:"geofence_id"`
	GeofenceName string  `json:"geofence_name"`
	EventType    string  `json:"event_type"`
	EventTime    string  `json:"event_time"`
	LatencyNS    int64   `json:"latency_ns"`
}

const testGeofenceID = "00000000-0000-0000-0000-000000000101"

func main() {
	wsURL := flag.String("ws-url", "ws://localhost:8080/ws/ingest", "ingest ws endpoint")
	httpURL := flag.String("http-url", "http://localhost:8080", "ingest http base url, for the debug check")
	vehicleID := flag.String("vehicle", "route-test-1", "vehicle id for this run")
	steps := flag.Int("steps", 25, "number of points along the route")
	stepDelay := flag.Duration("step-delay", 300*time.Millisecond, "delay between points")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, *wsURL, nil)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()

	// The test geofence is the square lng in [3.395, 3.405], lat in
	// [6.525, 6.535]. Drive straight across it at mid height (lat 6.530),
	// starting well west and finishing well east, so there's an
	// unambiguous single enter and single exit.
	const lat = 6.530
	startLng, endLng := 3.380, 3.420

	fmt.Printf("sending %d pings for vehicle %s, lng %.4f -> %.4f at lat %.4f\n",
		*steps, *vehicleID, startLng, endLng, lat)

	for i := 0; i < *steps; i++ {
		frac := float64(i) / float64(*steps-1)
		lng := startLng + frac*(endLng-startLng)

		ping := PositionPing{
			VehicleID:  *vehicleID,
			Lat:        lat,
			Lon:        lng,
			RecordedAt: time.Now(),
		}
		if err := wsjson.Write(ctx, conn, ping); err != nil {
			log.Fatalf("write ping %d: %v", i, err)
		}
		time.Sleep(*stepDelay)
	}

	// Give the last ping's worker processing a moment to land before checking.
	time.Sleep(500 * time.Millisecond)

	events, err := fetchAlerts(*httpURL)
	if err != nil {
		log.Fatalf("fetch alerts: %v", err)
	}

	var enters, exits int
	var latencies []int64
	for _, e := range events {
		if e.VehicleID != *vehicleID || e.GeofenceID != testGeofenceID {
			continue
		}
		latencies = append(latencies, e.LatencyNS)
		switch e.EventType {
		case "enter":
			enters++
		case "exit":
			exits++
		}
		fmt.Printf("  %s %s at %s, latency %.1fms\n",
			e.EventType, e.GeofenceName, e.EventTime, float64(e.LatencyNS)/1e6)
	}

	fmt.Printf("\nresult: enters=%d exits=%d\n", enters, exits)
	if enters == 1 && exits == 1 {
		fmt.Println("PASS — exactly one enter and one exit recorded")
		os.Exit(0)
	}
	fmt.Println("FAIL — expected exactly one enter and one exit")
	os.Exit(1)
}

func fetchAlerts(baseURL string) ([]AlertEvent, error) {
	resp, err := http.Get(baseURL + "/debug/alerts")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var events []AlertEvent
	if err := json.Unmarshal(body, &events); err != nil {
		return nil, err
	}
	return events, nil
}
