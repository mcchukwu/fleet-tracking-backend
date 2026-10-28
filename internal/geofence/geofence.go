// Package geofence holds the in-memory containment check.
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

// Contains is the even-odd ray-casting test. Linear in the ring size,
// which is fine for yard-sized polygons.
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

// Cache holds geofences by tenant.
type Cache struct {
	mu       sync.RWMutex
	byTenant map[string][]Polygon
}

func NewCache(polygons ...Polygon) *Cache {
	byTenant := make(map[string][]Polygon)
	for _, polygon := range polygons {
		byTenant[polygon.TenantID] = append(byTenant[polygon.TenantID], polygon)
	}
	return &Cache{byTenant: byTenant}
}

// ContainingPolygons returns every zone of tenantID that contains pt.
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

// AllPolygonNames is for the startup log; it is not on the hot path.
func (c *Cache) AllPolygonNames() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var names []string
	for _, polys := range c.byTenant {
		for _, poly := range polys {
			names = append(names, poly.Name)
		}
	}
	return names
}

// Reload rebuilds the cache in place after a geofence change.
func (c *Cache) Reload(ctx context.Context, db *sql.DB) error {
	fresh, err := LoadFromPostgres(ctx, db)
	if err != nil {
		return err
	}
	c.replace(fresh.byTenant)
	return nil
}

func (c *Cache) replace(byTenant map[string][]Polygon) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byTenant = byTenant
}

// geoJSONPolygon is the ST_AsGeoJSON shape we read. Coordinates are
// [lng, lat]; holes are rejected by a CHECK constraint.
type geoJSONPolygon struct {
	Type        string        `json:"type"`
	Coordinates [][][]float64 `json:"coordinates"`
}

// LoadFromPostgres reads all geofences. There is no reload; restart to
// pick up changes.
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
