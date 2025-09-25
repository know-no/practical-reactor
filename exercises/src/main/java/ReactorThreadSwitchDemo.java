import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class ReactorThreadSwitchDemo {

    public static void main(String[] args) {
        System.out.println("程序开始，主线程: " + Thread.currentThread().getName());

        Flux.range(1, 10)
            // 使用 .log() 可以清晰地看到每个信号的细节，包括线程名
            .log()
            // publishOn 是关键。它告诉 Reactor，从这里开始，
            // 后续的 onNext, onComplete, onError 信号都应该在 parallel 调度器的某个线程上执行。
            .publishOn(Schedulers.parallel())
            .map(i -> {
                // 打印当前map操作所在的线程名
                System.out.println("map 正在处理元素: " + i + "，执行线程: " + Thread.currentThread().getName());
                
                // 模拟一些耗时的工作，这会给调度器机会去使用不同的线程来处理下一个元素
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                
                return "处理结果-" + i;
            })
            // 为了让程序在Flux执行完毕前不退出，我们阻塞等待最后一个元素。
            // 在实际应用中应避免阻塞，这里仅为演示。
            .blockLast(); 

        System.out.println("程序结束，主线程: " + Thread.currentThread().getName());
    }
}
