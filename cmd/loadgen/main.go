// Load generator, extended with a storm mode
// (reconnect-storm defence) and, now, device authentication: every
// simulated vehicle dials with an Authorization bearer token and an
// X-Device-ID header, matching what cmd/ingest now requires. Without
// -api-key set to a valid key for the target tenant, every connection
// will be rejected with 401, that's the server behaving correctly, not
// a bug in this tool.
//
// this drives the WebSocket simulator
// adapter, not a real-hardware interface.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// PositionPing carries no identity field, identity comes from the
// authenticated connection (X-Device-ID), matching cmd/ingest's wsPing.
type PositionPing struct {
	Lat        float64   `json:"lat"`
	Lon        float64   `json:"lon"`
	SpeedKPH   float64   `json:"speed_kph,omitempty"`
	HeadingDeg float64   `json:"heading_deg,omitempty"`
	RecordedAt time.Time `json:"recorded_at"`
}

type counters struct {
	connected, dialErrors, dialRejected, sent, sendErrors, reconnects atomic.Int64
}

func main() {
	url := flag.String("url", "ws://localhost:8080/ws/ingest", "target ws endpoint")
	apiKey := flag.String("api-key", "dev-local-only-key", "tenant API key (matches migrations/seed_dev.sql's dev key by default)")
	vehicles := flag.Int("vehicles", 5000, "number of simulated vehicles (concurrent connections)")
	interval := flag.Duration("interval", 7*time.Second, "average ping interval per vehicle")
	duration := flag.Duration("duration", 2*time.Minute, "total run time per vehicle")
	rampUp := flag.Duration("rampup", 10*time.Second, "time to open all connections over, to avoid a connection-storm at t=0")
	storm := flag.Duration("storm", 0, "if >0, force every vehicle to disconnect and reconnect roughly this often, simulating a carrier outage — proves the reconnect-storm defence")
	flag.Parse()

	var c counters
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), *duration+*rampUp+30*time.Second)
	defer cancel()

	perConnDelay := time.Duration(0)
	if *vehicles > 0 {
		perConnDelay = *rampUp / time.Duration(*vehicles)
	}

	for i := range *vehicles {
		wg.Add(1)
		go func(vehicleID int) {
			defer wg.Done()
			runVehicle(ctx, *url, *apiKey, vehicleID, *interval, *duration, *storm, &c)
		}(i)
		time.Sleep(perConnDelay)
	}

	log.Printf("all %d connection goroutines launched, running for ~%s", *vehicles, *duration)
	if *storm > 0 {
		log.Printf("storm mode: forcing reconnects roughly every %s", *storm)
	}
	wg.Wait()

	fmt.Printf(
		"connected=%d dial_errors=%d dial_rejected=%d reconnects=%d sent=%d send_errors=%d\n",
		c.connected.Load(), c.dialErrors.Load(), c.dialRejected.Load(),
		c.reconnects.Load(), c.sent.Load(), c.sendErrors.Load(),
	)
	if c.dialErrors.Load() > 0 && c.connected.Load() == 0 {
		fmt.Println("every dial failed — if these are 401s, check -api-key matches a seeded, " +
			"non-revoked key, and that the dev tenant's auto_register_devices is true if these " +
			"vehicle ids have never connected before.")
	}
	if *storm > 0 {
		fmt.Println("check the server's /metrics/ingest: `rejected` should be > 0 " +
			"(the storm actually hit the admission limiter) and `idle_closed` should " +
			"stay near 0 (reconnects succeeded rather than going half-open). " +
			"`dial_rejected` above counts this client's own 503s from the limiter — " +
			"expected during a storm, and the client backs off and retries rather than giving up.")
	} else {
		fmt.Println("compare `sent` above against the ingest server's /metrics/ingest " +
			"`received` and `dropped` counters — sent should equal received, and " +
			"dropped should be 0 for Phase 1 to pass.")
	}
}

// runVehicle runs one simulated vehicle for the full duration, split into
// segments of at most `storm` length (or the whole duration if storm is
// 0). At the end of each segment it closes the connection and redials.
func runVehicle(ctx context.Context, url, apiKey string, id int, interval, totalDuration, storm time.Duration, c *counters) {
	deviceID := fmt.Sprintf("sim-%d", id)
	overallDeadline := time.Now().Add(totalDuration)
	first := true
	for time.Now().Before(overallDeadline) {
		segment := time.Until(overallDeadline)
		if storm > 0 && storm < segment {
			segment = storm
		}
		if !first {
			c.reconnects.Add(1)
		}
		first = false

		conn, ok := dialWithBackoff(ctx, url, apiKey, deviceID, c)
		if !ok {
			return
		}
		c.connected.Add(1)
		runSegment(ctx, conn, interval, segment, c)
	}
}

// dialWithBackoff retries on a 503 from the admission-rate limiter
// (expected during a storm) with jittered exponential backoff. A 401/403
// (bad api key or unregistered device) is NOT retried, that's a
// configuration problem this tool can't fix by trying again.
func dialWithBackoff(ctx context.Context, url, apiKey, deviceID string, c *counters) (*websocket.Conn, bool) {
	backoff := 200 * time.Millisecond
	const maxBackoff = 5 * time.Second
	opts := &websocket.DialOptions{
		HTTPHeader: http.Header{
			"Authorization": []string{"Bearer " + apiKey},
			"X-Device-ID":   []string{deviceID},
		},
	}
	for {
		conn, resp, err := websocket.Dial(ctx, url, opts)
		if err == nil {
			return conn, true
		}
		if resp != nil && resp.StatusCode == http.StatusServiceUnavailable {
			c.dialRejected.Add(1)
		} else {
			c.dialErrors.Add(1)
			return nil, false
		}
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(backoff + time.Duration(rand.Int63n(int64(backoff)))):
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
}

func runSegment(ctx context.Context, conn *websocket.Conn, interval, segment time.Duration, c *counters) {
	defer conn.CloseNow()

	lat := 6.5244 + rand.Float64()*0.1
	lon := 3.3792 + rand.Float64()*0.1

	deadline := time.Now().Add(segment)
	for time.Now().Before(deadline) {
		wait := interval/2 + time.Duration(rand.Int63n(int64(interval)))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		if time.Now().After(deadline) {
			return
		}

		lat += (rand.Float64() - 0.5) * 0.001
		lon += (rand.Float64() - 0.5) * 0.001

		ping := PositionPing{
			Lat: lat, Lon: lon,
			SpeedKPH: rand.Float64() * 80, HeadingDeg: rand.Float64() * 360,
			RecordedAt: time.Now(),
		}

		writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := wsjson.Write(writeCtx, conn, ping)
		cancel()
		if err != nil {
			c.sendErrors.Add(1)
			return
		}
		c.sent.Add(1)
	}
}
