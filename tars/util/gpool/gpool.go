package gpool

// Worker goroutine struct.
// ywl 按理协程池非常简单的，一个 任务 chan ，n 个协程从中取出任务，然后执行即可。估计这样 chan 内部争抢锁激烈，性能差。
// ywl 所以这里设计得好复杂，但仍然没有很好理解其精髓。
// ywl 但是好像又是为了实现标准库里已经有  context  cancel 功能。
type Worker struct {
	WorkerQueue chan *Worker // ywl: 这个设计为 成员变量好不舒服，我会让 start() 传参进来更好
	JobChannel  chan Job
	Stop        chan struct{}
}

// Start start gotoutine pool.
func (w *Worker) Start() {
	go func() {
		var job Job
		for {
			w.WorkerQueue <- w // ywl: 塞进去，表示有空
			select {
			case job = <-w.JobChannel:
				job() // ywl: 执行任务
			case <-w.Stop:
				w.Stop <- struct{}{}
				return
			}
		}
	}()
}

func newWorker(pool chan *Worker) *Worker {
	return &Worker{
		WorkerQueue: pool,
		JobChannel:  make(chan Job),
		Stop:        make(chan struct{}),
	}
}

// Job is a function for doing jobs.
type Job func()

// Pool is goroutine pool config.
type Pool struct {
	JobQueue    chan Job     // ywl 算是对外 “接口”
	WorkerQueue chan *Worker // 空闲可用的 计算资源
	stop        chan struct{}
}

// NewPool news gotouine pool
func NewPool(numWorkers int, jobQueueLen int) *Pool {
	jobQueue := make(chan Job, jobQueueLen)
	workerQueue := make(chan *Worker, numWorkers)

	pool := &Pool{
		JobQueue:    jobQueue,    // ywl:只是暂存 task 的缓冲而已
		WorkerQueue: workerQueue, // ywl:真正的并行度，woker个数(协程个数)
		stop:        make(chan struct{}),
	}
	pool.Start()
	return pool
}

// Start starts all workers
func (p *Pool) Start() {
	for i := 0; i < cap(p.WorkerQueue); i++ {
		worker := newWorker(p.WorkerQueue)
		worker.Start() // ywl: Start() 里面把 worker 放进 WorkerQueue; 这里设计得反直觉，按理要直接把 new 出来的 worker 当场放进 WorkerQueue。
	}

	go p.dispatch()
}

func (p *Pool) dispatch() {
	for {
		select {
		case job := <-p.JobQueue: // ywl: 外部的 task 会放这个 JobQueue 塞
			worker := <-p.WorkerQueue // ywl: 取得空闲的 Worker(协程)
			worker.JobChannel <- job  // ywl: 给予 task
		case <-p.stop:
			for i := 0; i < cap(p.WorkerQueue); i++ {
				worker := <-p.WorkerQueue

				worker.Stop <- struct{}{}
				<-worker.Stop
			}

			p.stop <- struct{}{}
			return
		}
	}
}

// Release release all workers
func (p *Pool) Release() {
	p.stop <- struct{}{}
	<-p.stop
}
