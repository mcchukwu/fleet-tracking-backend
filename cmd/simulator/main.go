package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type point struct{ lat, lon float64 }

var route = []point{
	{6.447, 3.390}, {6.470, 3.410}, {6.520, 3.450}, {6.580, 3.490},
	{6.650, 3.530}, {6.750, 3.580}, {6.850, 3.630}, {6.950, 3.680},
	{7.050, 3.720}, {7.150, 3.770}, {7.250, 3.820}, {7.370, 3.880},
}

type ping struct {
	Lat        float64   `json:"lat"`
	Lon        float64   `json:"lon"`
	SpeedKPH   float64   `json:"speed_kph"`
	HeadingDeg float64   `json:"heading_deg"`
	RecordedAt time.Time `json:"recorded_at"`
}

func main() {
	wsURL := flag.String("ws-url", "ws://localhost:8080/ws/ingest", "ingest endpoint")
	apiKey := flag.String("api-key", "demo-device-key", "tenant API key")
	count := flag.Int("count", 20, "number of vehicles")
	interval := flag.Duration("interval", 2*time.Second, "ping interval")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	for i := 0; i < *count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runVehicle(ctx, *wsURL, *apiKey, i, *interval)
		}(i)
	}
	wg.Wait()
}

func runVehicle(ctx context.Context, wsURL, apiKey string, index int, interval time.Duration) {
	deviceID := fmt.Sprintf("demo-imei-%04d", index+1)
	log := slog.With("device", deviceID)

	for ctx.Err() == nil {
		conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
			HTTPHeader: map[string][]string{
				"Authorization": {"Bearer " + apiKey},
				"X-Device-ID":   {deviceID},
			},
		})
		if err != nil {
			log.Warn("dial failed, retrying", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
			continue
		}
		stream(ctx, conn, index, interval)
		conn.CloseNow()
	}
}

func stream(ctx context.Context, conn *websocket.Conn, index int, interval time.Duration) {
	step := 0
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pos := positionAt(index, step)
			next := positionAt(index, step+1)
			heading := math.Atan2(next.lon-pos.lon, next.lat-pos.lat) * 180 / math.Pi
			if heading < 0 {
				heading += 360
			}
			err := wsjson.Write(ctx, conn, ping{
				Lat: pos.lat, Lon: pos.lon, SpeedKPH: 48,
				HeadingDeg: heading, RecordedAt: time.Now().UTC(),
			})
			if err != nil {
				return
			}
			step++
		}
	}
}

// positionAt walks the route forward, then back, with each vehicle offset so
// the fleet is spread along the road instead of moving as one block.
func positionAt(vehicle, step int) point {
	const stepsPerLeg = 240
	total := stepsPerLeg * 2
	pos := (step + vehicle*17) % total
	if pos >= stepsPerLeg {
		pos = total - pos
	}
	progress := float64(pos) / stepsPerLeg * float64(len(route)-1)
	i := int(progress)
	if i >= len(route)-1 {
		return route[len(route)-1]
	}
	frac := progress - float64(i)
	a, b := route[i], route[i+1]
	return point{
		lat: a.lat + (b.lat-a.lat)*frac,
		lon: a.lon + (b.lon-a.lon)*frac,
	}
}
