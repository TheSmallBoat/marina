package marina

import (
	"sync/atomic"
)

const defaultWorkerPoolSize = 32 // The default Pool size for the single task queue

type workerPool struct {
	maxWorkers  uint16
	taskCounter uint32
	taskQueue   []chan func()
	exit        []chan struct{}
}

func NewWorkerPool(maxWorkers uint16) *workerPool {
	// There must be at least one worker.
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	wch := &workerPool{
		maxWorkers:  maxWorkers,
		taskCounter: uint32(0),
		taskQueue:   make([]chan func(), maxWorkers),
		exit:        make([]chan struct{}, maxWorkers),
	}

	// Start the task dispatcher.
	wch.dispatch()

	return wch
}

func (wc *workerPool) executeTask(taskId uint16) {
	go func() {
		for {
			select {
			case task, ok := <-wc.taskQueue[taskId]:
				if ok && task != nil {
					// Execute the task.
					task()
				}
			case <-wc.exit[taskId]:
				return
			}
		}
	}()
}

func (wc *workerPool) dispatch() {
	for i := uint16(0); i < wc.maxWorkers; i++ {
		wc.taskQueue[i] = make(chan func(), defaultWorkerPoolSize)
		wc.exit[i] = make(chan struct{}, 0)
		wc.executeTask(i)
	}
}

func (wc *workerPool) Close() {
	for i := uint16(0); i < wc.maxWorkers; i++ {
		wc.exit[i] <- struct{}{}
	}
}

func (wc *workerPool) getId() uint16 {
	atomic.AddUint32(&wc.taskCounter, uint32(1))
	return uint16(wc.taskCounter % uint32(wc.maxWorkers))
}

func (wc *workerPool) SubmitTask(task func()) {
	if task != nil {
		wc.taskQueue[wc.getId()] <- task
	}
}
