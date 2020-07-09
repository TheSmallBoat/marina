package marina

import (
	"sync/atomic"
)

const defaultTaskPoolSize = 32 // The default Pool size for the single task queue

type taskPool struct {
	maxWorkers  uint16
	taskCounter uint32
	taskQueue   []chan func()
	exit        []chan struct{}
}

func NewTaskPool(maxWorkers uint16) *taskPool {
	// There must be at least one worker.
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	tp := &taskPool{
		maxWorkers:  maxWorkers,
		taskCounter: uint32(0),
		taskQueue:   make([]chan func(), maxWorkers),
		exit:        make([]chan struct{}, maxWorkers),
	}

	// Start the task dispatcher.
	tp.dispatch()

	return tp
}

func (tp *taskPool) executeTask(taskId uint16) {
	go func() {
		for {
			select {
			case task, ok := <-tp.taskQueue[taskId]:
				if ok && task != nil {
					// Execute the task.
					task()
				}
			case <-tp.exit[taskId]:
				return
			}
		}
	}()
}

func (tp *taskPool) dispatch() {
	for i := uint16(0); i < tp.maxWorkers; i++ {
		tp.taskQueue[i] = make(chan func(), defaultTaskPoolSize)
		tp.exit[i] = make(chan struct{}, 0)
		tp.executeTask(i)
	}
}

func (tp *taskPool) Close() {
	for i := uint16(0); i < tp.maxWorkers; i++ {
		tp.exit[i] <- struct{}{}
	}
}

func (tp *taskPool) getId() uint16 {
	atomic.AddUint32(&tp.taskCounter, uint32(1))
	return uint16(tp.taskCounter % uint32(tp.maxWorkers))
}

func (tp *taskPool) SubmitTask(task func()) {
	if task != nil {
		tp.taskQueue[tp.getId()] <- task
	}
}
