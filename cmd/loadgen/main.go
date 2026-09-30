// Load generator, extended with a storm mode
// to actually exercise the reconnect-storm and half-open-detection
// hardening added to the ingest server: this drives the WebSocket simulator
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

type PositionPing struct {
	VehicleID  string    `json:"vehicle_id"`
	Lat        float64   `json:"lat"`
	Lon        float64   `json:"lon"`
	SpeedKPH   float64   `json:"speed_kph,omitempty"`
	HeadingDeg float64   `json:"heading_deg,omitempty"`
	RecordedAt time.Time `json:"recorded_at"`
}

func main() {
	url := flag.String("url", "ws://localhost:8080/ws/ingest", "target ws endpoint")
	vehicles := flag.Int("vehicles", 5000, "number of simulated vehicles (concurrent connections)")
	interval := flag.Duration("interval", 7*time.Second, "average ping interval per vehicle")
	duration := flag.Duration("duration", 2*time.Minute, "total run time per vehicle")
	rampUp := flag.Duration("rampup", 10*time.Second, "time to open all connections over, to avoid a connection-storm at t=0")
	storm := flag.Duration("storm", 0, "if >0, force every vehicle to disconnect and reconnect roughly this often, simulating a carrier outage — proves the reconnect-storm defence")
	flag.Parse()

	var connected, dialErrors, dialRejected, sent, sendErrors, reconnects int64
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), *duration+*rampUp+30*time.Second)
	defer cancel()

	perConnDelay := time.Duration(0)
	if *vehicles > 0 {
		perConnDelay = *rampUp / time.Duration(*vehicles)
	}

	for i := 0; i < *vehicles; i++ {
		wg.Add(1)
		go func(vehicleID int) {
			defer wg.Done()
			runVehicle(ctx, *url, vehicleID, *interval, *duration, *storm,
				&connected, &dialErrors, &dialRejected, &sent, &sendErrors, &reconnects)
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
		atomic.LoadInt64(&connected), atomic.LoadInt64(&dialErrors), atomic.LoadInt64(&dialRejected),
		atomic.LoadInt64(&reconnects), atomic.LoadInt64(&sent), atomic.LoadInt64(&sendErrors),
	)
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
// 0). At the end of each segment it closes the connection and redials,
// this is what "force every vehicle to reconnect at once" means in
// practice: not a shared broadcast signal, just every vehicle's own
// clock reaching the same interval at roughly the same time, since they
// all started within the same rampUp window. That's a closer analogue to
// a real regional carrier outage than a perfectly synchronised signal
// would be anyway. Real devices don't drop in the same nanosecond either.
func runVehicle(
	ctx context.Context, url string, id int, interval, totalDuration, storm time.Duration,
	connected, dialErrors, dialRejected, sent, sendErrors, reconnects *int64,
) {
	overallDeadline := time.Now().Add(totalDuration)
	first := true
	for time.Now().Before(overallDeadline) {
		segment := time.Until(overallDeadline)
		if storm > 0 && storm < segment {
			segment = storm
		}
		if !first {
			atomic.AddInt64(reconnects, 1)
		}
		first = false

		conn, ok := dialWithBackoff(ctx, url, dialErrors, dialRejected)
		if !ok {
			return
		}
		atomic.AddInt64(connected, 1)
		runSegment(ctx, conn, id, interval, segment, sent, sendErrors)
	}
}

// dialWithBackoff retries on a 503 from the admission-rate limiter
// (expected and correct behaviour during a storm) with jittered
// exponential backoff, rather than treating a throttled connection
// attempt as a failure. A real device's firmware does something
// equivalent: it's what makes the limiter's slowdown effective instead
// of just relocating the storm to the reconnect loop.
func dialWithBackoff(ctx context.Context, url string, dialErrors, dialRejected *int64) (*websocket.Conn, bool) {
	backoff := 200 * time.Millisecond
	const maxBackoff = 5 * time.Second
	for {
		conn, resp, err := websocket.Dial(ctx, url, nil)
		if err == nil {
			return conn, true
		}
		if resp != nil && resp.StatusCode == http.StatusServiceUnavailable {
			atomic.AddInt64(dialRejected, 1)
		} else {
			atomic.AddInt64(dialErrors, 1)
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

func runSegment(
	ctx context.Context, conn *websocket.Conn, id int, interval, segment time.Duration,
	sent, sendErrors *int64,
) {
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
			VehicleID:  fmt.Sprintf("sim-%d", id),
			Lat:        lat,
			Lon:        lon,
			SpeedKPH:   rand.Float64() * 80,
			HeadingDeg: rand.Float64() * 360,
			RecordedAt: time.Now(),
		}

		writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := wsjson.Write(writeCtx, conn, ping)
		cancel()
		if err != nil {
			atomic.AddInt64(sendErrors, 1)
			return
		}
		atomic.AddInt64(sent, 1)
	}
}
