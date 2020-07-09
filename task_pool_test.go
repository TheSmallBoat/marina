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

func TestTaskPool(t *testing.T) {
	defer goleak.VerifyNone(t)

	var tp0 = NewTaskPool(0)
	defer tp0.Close()

	var tp = NewTaskPool(8)
	defer tp.Close()

	require.Equal(t, tp.maxWorkers, uint16(8))

	for i := 0; i < 1024; i++ {
		wg.Add(1)
		time.Sleep(1 * time.Nanosecond)
		tp.SubmitTask(myFunc)
	}
	wg.Wait()
	require.Equal(t, tp.taskCounter*uint32(step), uint32(sum))
}

func BenchmarkTaskPool(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	var tp = NewTaskPool(8)
	defer tp.Close()

	require.Equal(b, tp.maxWorkers, uint16(8))

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		tp.SubmitTask(myFunc)
	}
	wg.Wait()
}
