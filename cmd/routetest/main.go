// routetest runs two correctness checks against a live ingest server:
//
//  1. A deliberate straight-line route through the seeded "Test Yard"
//     geofence, checking for exactly one enter and one exit.
//  2. A single deliberately stale ping sent after the route finishes,
//     an old timestamp, and a position that would (wrongly) look like a
//     second "enter" if the server applied it, checking that neither
//     the hot-path position nor the alert log are affected by it.
//
// This is the correctness check the stochastic load generator can't
// give you: with 5,000 randomly-drifting vehicles you can't predict who
// crosses what, when, or engineer a precise out-of-order scenario. This
// tool trades scale for a known-correct answer.
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
	VehicleID    string `json:"vehicle_id"`
	GeofenceID   string `json:"geofence_id"`
	GeofenceName string `json:"geofence_name"`
	EventType    string `json:"event_type"`
	EventTime    string `json:"event_time"`
	LatencyNS    int64  `json:"latency_ns"`
}

const testGeofenceID = "00000000-0000-0000-0000-000000000101"

func main() {
	wsURL := flag.String("ws-url", "ws://localhost:8080/ws/ingest", "public ingest ws endpoint")
	adminURL := flag.String("admin-url", "http://localhost:9090", "internal admin http base url (metrics/debug)")
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
	// [6.525, 6.535]. Drive straight across it at mid-height (lat 6.530),
	// starting well west and finishing well east.
	const lat = 6.530
	startLng, endLng := 3.380, 3.420

	fmt.Printf("=== crossing test ===\nsending %d pings for vehicle %s, lng %.4f -> %.4f at lat %.4f\n",
		*steps, *vehicleID, startLng, endLng, lat)

	var lastRecordedAt time.Time
	for i := 0; i < *steps; i++ {
		frac := float64(i) / float64(*steps-1)
		lng := startLng + frac*(endLng-startLng)
		now := time.Now()
		lastRecordedAt = now

		ping := PositionPing{VehicleID: *vehicleID, Lat: lat, Lon: lng, RecordedAt: now}
		if err := wsjson.Write(ctx, conn, ping); err != nil {
			log.Fatalf("write ping %d: %v", i, err)
		}
		time.Sleep(*stepDelay)
	}
	time.Sleep(500 * time.Millisecond) // let the last ping's worker processing land

	enters, exits, _ := countEvents(*adminURL, *vehicleID)
	fmt.Printf("result: enters=%d exits=%d\n", enters, exits)
	crossingPass := enters == 1 && exits == 1
	printResult("crossing test", crossingPass)

	// === stale-ping test ===
	// Send one ping with an old timestamp (well before every ping just
	// sent) and a position INSIDE the geofence, the position most likely
	// to expose a bug, since a naive implementation would see "not
	// currently in the inside-set" and wrongly fire a second "enter".
	fmt.Println("\n=== stale-ping test ===")
	stalePing := PositionPing{
		VehicleID:  *vehicleID,
		Lat:        lat,
		Lon:        3.400, // inside the box
		RecordedAt: lastRecordedAt.Add(-30 * time.Second),
	}
	fmt.Printf("sending one ping timestamped %s before the last real one, positioned inside the geofence\n",
		lastRecordedAt.Sub(stalePing.RecordedAt))
	if err := wsjson.Write(ctx, conn, stalePing); err != nil {
		log.Fatalf("write stale ping: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	state, err := fetchVehicleState(*adminURL, *vehicleID)
	if err != nil {
		log.Fatalf("fetch vehicle state: %v", err)
	}
	fmt.Printf("current hot-path state: %v\n", state)
	positionUnaffected := state["lon"] != "" && !closeEnough(state["lon"], "3.400")

	enters2, exits2, _ := countEvents(*adminURL, *vehicleID)
	fmt.Printf("result after stale ping: enters=%d exits=%d (should be unchanged from before)\n", enters2, exits2)
	noSpuriousAlert := enters2 == enters && exits2 == exits

	stalePass := positionUnaffected && noSpuriousAlert
	printResult("stale-ping test", stalePass)

	if !crossingPass || !stalePass {
		os.Exit(1)
	}
}

func printResult(name string, pass bool) {
	if pass {
		fmt.Printf("PASS — %s\n", name)
	} else {
		fmt.Printf("FAIL — %s\n", name)
	}
}

func countEvents(adminURL, vehicleID string) (enters, exits int, err error) {
	events, err := fetchAlerts(adminURL)
	if err != nil {
		return 0, 0, err
	}
	for _, e := range events {
		if e.VehicleID != vehicleID || e.GeofenceID != testGeofenceID {
			continue
		}
		switch e.EventType {
		case "enter":
			enters++
		case "exit":
			exits++
		}
	}
	return enters, exits, nil
}

func fetchAlerts(adminURL string) ([]AlertEvent, error) {
	resp, err := http.Get(adminURL + "/debug/alerts")
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

func fetchVehicleState(adminURL, vehicleID string) (map[string]string, error) {
	resp, err := http.Get(adminURL + "/debug/vehicle?id=" + vehicleID)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var state map[string]string
	if err := json.Unmarshal(body, &state); err != nil {
		return nil, err
	}
	return state, nil
}

// closeEnough does a crude string-prefix comparison, good enough for a
// test tool distinguishing "3.42-ish" from "3.400", not a general
// float-comparison utility.
func closeEnough(a, b string) bool {
	n := len(b)
	if len(a) < n {
		n = len(a)
	}
	return a[:n] == b[:n]
}
