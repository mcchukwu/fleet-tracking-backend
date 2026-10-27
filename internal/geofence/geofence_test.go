package geofence

import "testing"

func TestPolygonContains(t *testing.T) {
	polygon := Polygon{Ring: []Point{
		{Lng: 0, Lat: 0}, {Lng: 10, Lat: 0}, {Lng: 10, Lat: 10}, {Lng: 0, Lat: 10}, {Lng: 0, Lat: 0},
	}}

	for _, test := range []struct {
		name  string
		point Point
		want  bool
	}{
		{"inside", Point{Lng: 5, Lat: 5}, true},
		{"outside", Point{Lng: 11, Lat: 5}, false},
		{"empty polygon", Point{Lng: 0, Lat: 0}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := polygon
			if test.name == "empty polygon" {
				candidate.Ring = nil
			}
			if got := candidate.Contains(test.point); got != test.want {
				t.Fatalf("Contains(%+v) = %v, want %v", test.point, got, test.want)
			}
		})
	}
}

func TestCacheSeparatesTenants(t *testing.T) {
	cache := NewCache(
		Polygon{ID: "one", TenantID: "tenant-a", Ring: square()},
		Polygon{ID: "two", TenantID: "tenant-b", Ring: square()},
	)
	if got := cache.ContainingPolygons("tenant-a", Point{Lng: 5, Lat: 5}); len(got) != 1 || got[0].ID != "one" {
		t.Fatalf("tenant-a polygons = %#v", got)
	}
	if got := cache.ContainingPolygons("tenant-c", Point{Lng: 5, Lat: 5}); len(got) != 0 {
		t.Fatalf("unknown tenant polygons = %#v", got)
	}
}

func square() []Point {
	return []Point{{0, 0}, {10, 0}, {10, 10}, {0, 10}, {0, 0}}
}
