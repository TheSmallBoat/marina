package marina

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

const step = 8

var sum = int32(0)
var wg sync.WaitGroup

func myFunc() {
	n := int32(step)
	atomic.AddInt32(&sum, n)
	time.Sleep(1 * time.Microsecond)
	wg.Done()
}

func TestWorkerPool(t *testing.T) {
	defer goleak.VerifyNone(t)

	var wc0 = NewWorkingPool(0)
	defer wc0.Close()

	var wc = NewWorkingPool(8)
	defer wc.Close()

	require.Equal(t, wc.maxWorkers, uint16(8))

	for i := 0; i < 1024; i++ {
		wg.Add(1)
		time.Sleep(1 * time.Nanosecond)
		wc.SubmitTask(myFunc)
	}
	wg.Wait()
	require.Equal(t, wc.taskCounter*uint32(step), uint32(sum))
}

func BenchmarkWorkerPool(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	var wc = NewWorkingPool(8)
	defer wc.Close()

	require.Equal(b, wc.maxWorkers, uint16(8))

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		wc.SubmitTask(myFunc)
	}
	wg.Wait()
}
