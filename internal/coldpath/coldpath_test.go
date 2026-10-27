package coldpath

import (
	"sync"
	"testing"
	"time"
)

func TestBatchLoopFlushesRemainingRecordsWhenClosed(t *testing.T) {
	ch := make(chan int, 3)
	ch <- 1
	ch <- 2
	close(ch)

	var mu sync.Mutex
	var batches [][]int
	done := make(chan struct{})
	go func() {
		batchLoop(ch, time.Hour, 3, func(batch []int) {
			copy := append([]int(nil), batch...)
			mu.Lock()
			batches = append(batches, copy)
			mu.Unlock()
		})
		close(done)
	}()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(batches) != 1 || len(batches[0]) != 2 || batches[0][0] != 1 || batches[0][1] != 2 {
		t.Fatalf("batches = %#v", batches)
	}
}
