package ratelimit

import "testing"

func TestPerIPIsolatesAddresses(t *testing.T) {
	p := NewPerIP(0, 2)

	if !p.Allow("a") || !p.Allow("a") {
		t.Fatal("first burst for a should pass")
	}
	if p.Allow("a") {
		t.Fatal("third request for a should be rejected")
	}
	if !p.Allow("b") {
		t.Fatal("b must have its own bucket")
	}
}
