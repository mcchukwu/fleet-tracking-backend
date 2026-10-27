package pipeline

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/mcchukwu/fleet-tracking-backend/internal/geofence"
	"github.com/redis/go-redis/v9"
)

func TestAlertLogSnapshotPreservesChronologicalOrderAfterWrap(t *testing.T) {
	log := newAlertLog(3)
	for i := range 5 {
		log.record(AlertEvent{VehicleID: string(rune('a' + i)), EventTime: time.Unix(int64(i), 0)})
	}

	events := log.snapshot()
	if len(events) != 3 {
		t.Fatalf("snapshot length = %d, want 3", len(events))
	}
	for i, want := range []string{"c", "d", "e"} {
		if events[i].VehicleID != want {
			t.Fatalf("event %d = %q, want %q", i, events[i].VehicleID, want)
		}
	}
}

func TestDropPoliciesAccountForDiscardedPings(t *testing.T) {
	for _, test := range []struct {
		name      string
		policy    string
		queuedID  string
		dropCount int64
	}{
		{"drop newest", "drop_newest", "first", 1},
		{"drop oldest", "drop_oldest", "second", 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := New(nil, geofence.NewCache(), Config{OverflowPolicy: test.policy, ChannelBufferSize: 1}, slog.Default())
			p.Submit(PositionPing{VehicleID: "first"})
			p.Submit(PositionPing{VehicleID: "second"})

			if got := p.Dropped(); got != test.dropCount {
				t.Fatalf("dropped = %d, want %d", got, test.dropCount)
			}
			if got := (<-p.ingestCh).VehicleID; got != test.queuedID {
				t.Fatalf("queued vehicle = %q, want %q", got, test.queuedID)
			}
		})
	}
}

func TestApplyPositionAtomicallyTracksTransitions(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR is not set")
	}

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("TEST_REDIS_PASSWORD")})
	defer rdb.Close()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush Redis: %v", err)
	}

	polygon := geofence.Polygon{
		ID: "yard", TenantID: "tenant", Name: "Yard",
		Ring: []geofence.Point{{Lng: 0, Lat: 0}, {Lng: 10, Lat: 0}, {Lng: 10, Lat: 10}, {Lng: 0, Lat: 10}, {Lng: 0, Lat: 0}},
	}
	p := New(rdb, geofence.NewCache(polygon), Config{ChannelBufferSize: 1, WorkerPoolSize: 1}, slog.Default())
	base := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)

	inside := PositionPing{TenantID: "tenant", VehicleID: "vehicle", Lat: 5, Lon: 5, RecordedAt: base}
	applied, entered, exited, err := p.applyPosition(ctx, inside, p.containingPolygons(inside))
	if err != nil || !applied || len(entered) != 1 || entered[0] != "yard" || len(exited) != 0 {
		t.Fatalf("inside apply = applied:%v entered:%v exited:%v err:%v", applied, entered, exited, err)
	}

	stale := inside
	stale.RecordedAt = base.Add(-time.Nanosecond)
	applied, entered, exited, err = p.applyPosition(ctx, stale, p.containingPolygons(stale))
	if err != nil || applied || len(entered) != 0 || len(exited) != 0 {
		t.Fatalf("stale apply = applied:%v entered:%v exited:%v err:%v", applied, entered, exited, err)
	}

	outside := PositionPing{TenantID: "tenant", VehicleID: "vehicle", Lat: 15, Lon: 15, RecordedAt: base.Add(time.Nanosecond)}
	applied, entered, exited, err = p.applyPosition(ctx, outside, p.containingPolygons(outside))
	if err != nil || !applied || len(entered) != 0 || len(exited) != 1 || exited[0] != "yard" {
		t.Fatalf("outside apply = applied:%v entered:%v exited:%v err:%v", applied, entered, exited, err)
	}
}

// Two tenants can register the same external vehicle id. Their hot state
// must not collide, and one tenant's timestamp must not make the other's
// ping look stale.
func TestTenantScopedStateDoesNotCollide(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR is not set")
	}

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("TEST_REDIS_PASSWORD")})
	defer rdb.Close()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush Redis: %v", err)
	}

	ring := []geofence.Point{{Lng: 0, Lat: 0}, {Lng: 10, Lat: 0}, {Lng: 10, Lat: 10}, {Lng: 0, Lat: 10}, {Lng: 0, Lat: 0}}
	a := geofence.Polygon{ID: "yard-a", TenantID: "tenant-a", Name: "A", Ring: ring}
	b := geofence.Polygon{ID: "yard-b", TenantID: "tenant-b", Name: "B", Ring: ring}
	p := New(rdb, geofence.NewCache(a, b), Config{ChannelBufferSize: 1, WorkerPoolSize: 1}, slog.Default())

	base := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	pingA := PositionPing{TenantID: "tenant-a", VehicleID: "same-id", Lat: 5, Lon: 5, RecordedAt: base}
	if applied, entered, _, err := p.applyPosition(ctx, pingA, p.containingPolygons(pingA)); err != nil || !applied || len(entered) != 1 || entered[0] != "yard-a" {
		t.Fatalf("tenant A: applied=%v entered=%v err=%v", applied, entered, err)
	}

	pingB := PositionPing{TenantID: "tenant-b", VehicleID: "same-id", Lat: 6, Lon: 6, RecordedAt: base.Add(-time.Hour)}
	applied, entered, _, err := p.applyPosition(ctx, pingB, p.containingPolygons(pingB))
	if err != nil || !applied || len(entered) != 1 || entered[0] != "yard-b" {
		t.Fatalf("tenant B ping must not be stale against tenant A: applied=%v entered=%v err=%v", applied, entered, err)
	}

	for _, key := range []string{
		"tenant:tenant-a:vehicle:same-id:state",
		"tenant:tenant-b:vehicle:same-id:state",
	} {
		if n, err := rdb.Exists(ctx, key).Result(); err != nil || n != 1 {
			t.Fatalf("key %s: exists=%d err=%v", key, n, err)
		}
	}
}
