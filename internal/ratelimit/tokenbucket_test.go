package ratelimit

import (
	"testing"
	"time"
)

func TestTokenBucketHonorsBurstAndRefills(t *testing.T) {
	bucket := NewTokenBucket(10, 2)
	if !bucket.Allow() || !bucket.Allow() {
		t.Fatal("initial burst tokens were not accepted")
	}
	if bucket.Allow() {
		t.Fatal("bucket accepted a request after its burst was exhausted")
	}

	bucket.last = time.Now().Add(-100 * time.Millisecond)
	if !bucket.Allow() {
		t.Fatal("bucket did not refill after elapsed time")
	}
}
