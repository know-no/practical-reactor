import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

public class ReactorParallelismDemo {

    public static void main(String[] args) {
        System.out.println("程序开始，主线程: " + Thread.currentThread().getName());

        Flux.range(1, 10)
            .log()
            // flatMap会将每个元素i映射到一个新的Mono
            .flatMap(i -> 
                Mono.fromCallable(() -> {
                    // 这是每个元素要执行的“耗时任务”
                    System.out.println("-> 开始处理元素: " + i + "，执行线程: " + Thread.currentThread().getName());
//                    try {
                        // 模拟一个更长的耗时操作，比如I/O等待
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        Thread.currentThread().interrupt();
//                    }
                    System.out.println("<- 完成处理元素: " + i);
                    return "结果-" + i;
                })
                // 这是关键！subscribeOn告诉这个内部的Mono，
                // 它的整个执行过程（从订阅到产生结果）都应该在parallel调度器上。
                .subscribeOn(Schedulers.parallel())
            )
            .blockLast(); // 仅为演示，阻塞主线程等待所有任务完成

        System.out.println("程序结束，主线程: " + Thread.currentThread().getName());
    }
}
