// Package geofence holds the in-memory containment engine. This is
// deliberately NOT a PostGIS query per ping. Postgres is the system of
// record for geofence *definitions* (loaded once at startup, refreshed
// on a slow interval), but the actual "is this point inside this
// polygon" check that sits in the sub-100ms hot path runs entirely
// in-process against data already in memory.
package geofence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
)

type Point struct {
	Lng, Lat float64
}

type Polygon struct {
	ID       string
	TenantID string
	Name     string
	Ring     []Point // exterior ring only, see LoadFromPostgres for why
}

// Contains reports whether pt lies inside the polygon, using the
// even-odd ray-casting test (a.k.a. the PNPOLY algorithm): imagine a
// horizontal ray from pt heading toward +longitude infinity, and count
// how many polygon edges it crosses. Cross an odd number of edges and
// you're inside; an even number (including zero) and you're outside,
// the same parity argument as "did I cross the wall an odd or even
// number of times to get from outside to here."
//
// This is a linear scan over the ring's vertices, which is the right
// amount of algorithm for a yard/depot boundary with a handful of
// points. It stops being the right amount of algorithm if geofences
// grow to hundreds of vertices or there are thousands of them per
// tenant to check against on every ping, that's a bounding-box
// pre-filter or a spatial index, and it's not needed yet.
func (p Polygon) Contains(pt Point) bool {
	inside := false
	n := len(p.Ring)
	for i, j := 0, n-1; i < n; j, i = i, i+1 {
		vi, vj := p.Ring[i], p.Ring[j]
		if (vi.Lat > pt.Lat) != (vj.Lat > pt.Lat) {
			xIntersect := vj.Lng + (pt.Lat-vj.Lat)/(vi.Lat-vj.Lat)*(vi.Lng-vj.Lng)
			if pt.Lng < xIntersect {
				inside = !inside
			}
		}
	}
	return inside
}

// Cache holds every tenant's geofences in memory, keyed by tenant so a
// lookup for a given vehicle's ping only scans that tenant's polygons,
// not every geofence in the system.
type Cache struct {
	mu       sync.RWMutex
	byTenant map[string][]Polygon
}

func NewCache() *Cache {
	return &Cache{byTenant: make(map[string][]Polygon)}
}

// ContainingPolygons returns every polygon belonging to tenantID that
// contains pt. A vehicle is usually inside zero or one geofence, so this
// is a short slice in practice, but overlapping zones are legal and
// handled correctly, each is evaluated independently.
func (c *Cache) ContainingPolygons(tenantID string, pt Point) []Polygon {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var hits []Polygon
	for _, poly := range c.byTenant[tenantID] {
		if poly.Contains(pt) {
			hits = append(hits, poly)
		}
	}
	return hits
}

func (c *Cache) AllPolygonIDs(tenantID string) map[string]Polygon {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]Polygon, len(c.byTenant[tenantID]))
	for _, poly := range c.byTenant[tenantID] {
		out[poly.ID] = poly
	}
	return out
}

func (c *Cache) replace(byTenant map[string][]Polygon) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byTenant = byTenant
}

// geoJSONPolygon mirrors the subset of GeoJSON Postgres's ST_AsGeoJSON
// produces for a POLYGON: Coordinates[0] is the exterior ring,
// Coordinates[1:] would be holes (interior rings), which v1 ignores,
// fleet geofences are yards and depots, not shapes with donut holes in
// them. Note GeoJSON orders each point as [lng, lat], not [lat, lng].
// the opposite of how most people say coordinates out loud.
type geoJSONPolygon struct {
	Type        string        `json:"type"`
	Coordinates [][][]float64 `json:"coordinates"`
}

// LoadFromPostgres reads every geofence and builds a fresh Cache. Called
// once at startup; call it again on a timer or an admin-triggered reload
// if geofences need to change without a server restart, not built yet,
// because nothing has needed it yet. That's the gap, stated explicitly
// rather than silently missing.
func LoadFromPostgres(ctx context.Context, db *sql.DB) (*Cache, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, tenant_id, name, ST_AsGeoJSON(boundary)
		FROM geofences
	`)
	if err != nil {
		return nil, fmt.Errorf("query geofences: %w", err)
	}
	defer rows.Close()

	byTenant := make(map[string][]Polygon)
	for rows.Next() {
		var id, tenantID, name, geoJSON string
		if err := rows.Scan(&id, &tenantID, &name, &geoJSON); err != nil {
			return nil, fmt.Errorf("scan geofence row: %w", err)
		}

		var gj geoJSONPolygon
		if err := json.Unmarshal([]byte(geoJSON), &gj); err != nil {
			return nil, fmt.Errorf("parse geojson for geofence %s: %w", id, err)
		}
		if len(gj.Coordinates) == 0 {
			continue
		}

		ring := make([]Point, 0, len(gj.Coordinates[0]))
		for _, coord := range gj.Coordinates[0] {
			ring = append(ring, Point{Lng: coord[0], Lat: coord[1]})
		}

		byTenant[tenantID] = append(byTenant[tenantID], Polygon{
			ID: id, TenantID: tenantID, Name: name, Ring: ring,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate geofence rows: %w", err)
	}

	cache := NewCache()
	cache.replace(byTenant)
	return cache, nil
}
