package marina

import (
	"sync/atomic"
)

const defaultWorkingPoolSize = 32 // The default Pool size for the single task queue

type workingPool struct {
	maxWorkers  uint16
	taskCounter uint32
	taskQueue   []chan func()
	exit        []chan struct{}
}

func NewWorkingPool(maxWorkers uint16) *workingPool {
	// There must be at least one worker.
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	wch := &workingPool{
		maxWorkers:  maxWorkers,
		taskCounter: uint32(0),
		taskQueue:   make([]chan func(), maxWorkers),
		exit:        make([]chan struct{}, maxWorkers),
	}

	// Start the task dispatcher.
	wch.dispatch()

	return wch
}

func (wc *workingPool) executeTask(taskId uint16) {
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

func (wc *workingPool) dispatch() {
	for i := uint16(0); i < wc.maxWorkers; i++ {
		wc.taskQueue[i] = make(chan func(), defaultWorkingPoolSize)
		wc.exit[i] = make(chan struct{}, 0)
		wc.executeTask(i)
	}
}

func (wc *workingPool) Close() {
	for i := uint16(0); i < wc.maxWorkers; i++ {
		wc.exit[i] <- struct{}{}
	}
}

func (wc *workingPool) getId() uint16 {
	atomic.AddUint32(&wc.taskCounter, uint32(1))
	return uint16(wc.taskCounter % uint32(wc.maxWorkers))
}

func (wc *workingPool) SubmitTask(task func()) {
	if task != nil {
		wc.taskQueue[wc.getId()] <- task
	}
}
