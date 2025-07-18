package sinks;


import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamingTaskScheduler {

    private final BlockingQueue<StreamingTask> highPriorityQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<StreamingTask> lowPriorityQueue = new LinkedBlockingQueue<>();
    private final ExecutorService executorService;

    private int workCnt;
    private int lowAndHighPriorityCnt;

    public StreamingTaskScheduler() {
        this(10, 7);
    }

    public StreamingTaskScheduler(int workerCnt, int lowAndHighPriorityCnt) {
        // todo safe check
        this.workCnt = workerCnt;
        this.lowAndHighPriorityCnt = lowAndHighPriorityCnt;

        ThreadFactory factory = new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "StreamingTaskWorker-" + threadNumber.getAndIncrement());
                t.setUncaughtExceptionHandler((thread, ex) -> {
                    System.err.println("Thread " + thread.getName() + " terminated due to exception: " + ex);
                    // 重新提交任务以保持线程数量
                    executorService.execute(StreamingTaskScheduler.this::processTasks);
                });
                return t;
            }
        };

        this.executorService = new ThreadPoolExecutor(workerCnt, workerCnt, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), factory);

        for (int i = 0; i < this.workCnt; i++) {
            executorService.execute(this::processTasks);
        }
    }

    private void processTasks() {
        ThreadLocalRandom current = ThreadLocalRandom.current();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                StreamingTask task = current.nextInt() % 10 < lowAndHighPriorityCnt
                                     ? highPriorityQueue.poll(100, TimeUnit.MILLISECONDS)
                                     : lowPriorityQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task != null) {
                    task.run();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // 恢复中断状态,保守操作, 可删除
                break;
            }
        }
    }

    public void submit(StreamingTask task) {
        if (task.priority() < 50) {
            highPriorityQueue.offer(task);
        } else {
            lowPriorityQueue.offer(task);
        }
    }
}
