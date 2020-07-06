package marina

import (
	"reflect"
	"sync/atomic"
)

const defaultChannelSize = 32 // The default channel size for the single task queue

type workerChannel struct {
	maxWorkers  uint16
	taskCounter uint32
	taskQueue   []chan func()
	exit        []chan struct{}
}

func NewWorkerChannel(maxWorkers uint16) *workerChannel {
	// There must be at least one worker.
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	wch := &workerChannel{
		maxWorkers: maxWorkers,
		taskQueue:  make([]chan func(), maxWorkers),
		exit:       make([]chan struct{}, maxWorkers),
	}

	// Start the task dispatcher.
	wch.dispatch()

	return wch
}

func (wc *workerChannel) executeTask(taskId uint16) {
	go func() {
		for {
			select {
			case task, ok := <-wc.taskQueue[taskId]:
				if ok && task != nil && reflect.TypeOf(task) == reflect.TypeOf(func() {}) {
					// Execute the task.
					task()
				}
			case <-wc.exit[taskId]:
				return
			}
		}
	}()
}

func (wc *workerChannel) dispatch() {
	for i := uint16(0); i < wc.maxWorkers; i++ {
		wc.taskQueue[i] = make(chan func(), defaultChannelSize)
		wc.exit[i] = make(chan struct{}, 0)
		wc.executeTask(i)
	}
}

func (wc *workerChannel) Close() {
	for i := uint16(0); i < wc.maxWorkers; i++ {
		wc.exit[i] <- struct{}{}
	}
}

func (wc *workerChannel) getId() uint16 {
	atomic.AddUint32(&wc.taskCounter, uint32(1))
	return uint16(wc.taskCounter % uint32(wc.maxWorkers))
}

func (wc *workerChannel) SubmitTask(task func()) {
	if task != nil {
		wc.taskQueue[wc.getId()] <- task
	}
}
