package sinks;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class StreamingTask implements Runnable, PriorityComparable {

    private final RunnableWithException runnable;
    private final int priority;
    private final AtomicBoolean cancel;
    private final Consumer<Throwable> onError;

    public StreamingTask(RunnableWithException runnable, int priority, AtomicBoolean cancel, Consumer<Throwable> onError) {
        this.runnable = runnable;
        this.priority = priority;
        this.cancel = cancel;
        this.onError = onError;
    }

    @Override
    public void run() {
        if (!cancel.get()) {
            try {
                doOnRun();
                this.runnable.run();
                doPostRun();
            } catch (Throwable e) {
                this.onError.accept(e); // 一定要避免抛出异常, 实在需要抛出也没办法, 但是一定要发出异步终止的信号.
            } finally {

            }
        }

    }

    @Override
    public int compareTo(PriorityComparable o) {
        return 0;
    }

    @Override
    public int priority() {
        return 0;
    }

    private void doOnRun() {

    }

    private void doPostRun() {

    }

    public interface RunnableWithException {
        void run() throws Exception;

    }
}
