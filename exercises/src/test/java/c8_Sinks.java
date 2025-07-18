import org.junit.jupiter.api.*;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import sinks.StreamingTask;
import sinks.StreamingTaskScheduler;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In Reactor a Sink allows safe manual triggering of signals. We will learn more about multicasting and backpressure in
 * the next chapters.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#sinks
 * https://projectreactor.io/docs/core/release/reference/#processor-overview
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c8_Sinks extends SinksBase {

    /**
     * You need to execute operation that is submitted to legacy system which does not support Reactive API. You want to
     * avoid blocking and let subscribers subscribe to `operationCompleted` Mono, that will emit `true` once submitted
     * operation is executed by legacy system.
     */
    @Test
    public void single_shooter() {
        //todo: feel free to change code as you need
//        AtomicBoolean o = new AtomicBoolean(false);
        Sinks.One<Boolean> s = Sinks.one();
        Mono<Boolean> operationCompleted = s.asMono();

//            Mono.create((msk) -> {
//            System.out.println("defer");
//            try {
//                Thread.sleep(5500);
//            } catch (InterruptedException e) {
//                msk.error(new RuntimeException(e));
//                return;
//            }
//            msk.success(true);
//
//        });
        submitOperation(() -> {
            doSomeWork(); //don't change this line
//            o.set(true);
            s.tryEmitValue(true);
        });

        //don't change code below
        StepVerifier.create(operationCompleted.timeout(Duration.ofMillis(5500)))
                    .expectNext(true)
                    .verifyComplete();
    }

    /**
     * Similar to previous exercise, you need to execute operation that is submitted to legacy system which does not
     * support Reactive API. This time you need to obtain result of `get_measures_reading()` and emit it to subscriber.
     * If measurements arrive before subscribers subscribe to `get_measures_readings()`, buffer them and emit them to
     * subscribers once they are subscribed.
     */
    @Test
    public void single_subscriber() {
        //todo: feel free to change code as you need
        Sinks.Many<Integer> all = Sinks.many().replay().<Integer>all();
        Flux<Integer> measurements = all.asFlux();
        submitOperation(() -> {
            List<Integer> measures_readings = get_measures_readings(); //don't change this line
            for (Integer measure : measures_readings) {
                all.tryEmitNext(measure);
            }
            all.tryEmitComplete();
        });

        //don't change code below
        StepVerifier.create(measurements
                                    .delaySubscription(Duration.ofSeconds(6)))
                    .expectNext(0x0800, 0x0B64, 0x0504)
                    .verifyComplete();
    }

    /**
     * Same as previous exercise, but with twist that you need to emit measurements to multiple subscribers.
     * Subscribers should receive only the signals pushed through the sink after they have subscribed.
     */
    @Test
    public void it_gets_crowded() {
        //todo: feel free to change code as you need
        Sinks.Many<Integer> objectMany = Sinks.many().multicast().onBackpressureBuffer();
        Flux<Integer> measurements = objectMany.asFlux();
        submitOperation(() -> {

            List<Integer> measures_readings = get_measures_readings(); //don't change this line
            for (Integer measure : measures_readings) {
                objectMany.tryEmitNext(measure);
            }
            objectMany.tryEmitComplete();
        });

        //don't change code below
        StepVerifier.create(Flux.merge(measurements
                                               .delaySubscription(Duration.ofSeconds(1)),
                                       measurements.ignoreElements()))
                    .expectNext(0x0800, 0x0B64, 0x0504)
                    .verifyComplete();
    }

    /**
     * By default, if all subscribers have cancelled (which basically means they have all un-subscribed), sink clears
     * its internal buffer and stops accepting new subscribers. For this exercise, you need to make sure that if all
     * subscribers have cancelled, the sink will still accept new subscribers. Change this behavior by setting the
     * `autoCancel` parameter.
     */
    @Test
    public void open_24_7() {
        //todo: set autoCancel parameter to prevent sink from closing
        Sinks.Many<Integer> sink = Sinks.many().multicast().onBackpressureBuffer(
            Integer.MAX_VALUE, false
        );
        Flux<Integer> flux = sink.asFlux();

        //don't change code below
        submitOperation(() -> {
            get_measures_readings().forEach(sink::tryEmitNext);
            submitOperation(sink::tryEmitComplete);
        });

        //subscriber1 subscribes, takes one element and cancels
        StepVerifier sub1 = StepVerifier.create(Flux.merge(flux.take(1)))
                                        .expectNext(0x0800)
                                        .expectComplete()
                                        .verifyLater();

        //subscriber2 subscribes, takes one element and cancels
        StepVerifier sub2 = StepVerifier.create(Flux.merge(flux.take(1)))
                                        .expectNext(0x0800)
                                        .expectComplete()
                                        .verifyLater();

        //subscriber3 subscribes after all previous subscribers have cancelled
        StepVerifier sub3 = StepVerifier.create(flux.take(3)
                                                    .delaySubscription(Duration.ofSeconds(6)))
                                        .expectNext(0x0B64) //first measurement `0x0800` was already consumed by previous subscribers
                                        .expectNext(0x0504)
                                        .expectComplete()
                                        .verifyLater();

        sub1.verify();
        sub2.verify();
        sub3.verify();
    }

    /**
     * If you look closely, in previous exercises third subscriber was able to receive only two out of three
     * measurements. That's because used sink didn't remember history to re-emit all elements to new subscriber.
     * Modify solution from `open_24_7` so third subscriber will receive all measurements.
     */
    @Test
    public void blue_jeans() {
        //todo: enable autoCancel parameter to prevent sink from closing
        Sinks.Many<Integer> sink = Sinks.many().multicast().onBackpressureBuffer();
        Flux<Integer> flux = sink.asFlux();

        //don't change code below
        submitOperation(() -> {
            get_measures_readings().forEach(sink::tryEmitNext);
            submitOperation(sink::tryEmitComplete);
        });

        //subscriber1 subscribes, takes one element and cancels
        StepVerifier sub1 = StepVerifier.create(Flux.merge(flux.take(1)))
                                        .expectNext(0x0800)
                                        .expectComplete()
                                        .verifyLater();

        //subscriber2 subscribes, takes one element and cancels
        StepVerifier sub2 = StepVerifier.create(Flux.merge(flux.take(1)))
                                        .expectNext(0x0800)
                                        .expectComplete()
                                        .verifyLater();

        //subscriber3 subscribes after all previous subscribers have cancelled
        StepVerifier sub3 = StepVerifier.create(flux.take(3)
                                                    .delaySubscription(Duration.ofSeconds(6)))
                                        .expectNext(0x0800)
                                        .expectNext(0x0B64)
                                        .expectNext(0x0504)
                                        .expectComplete()
                                        .verifyLater();

        sub1.verify();
        sub2.verify();
        sub3.verify();
    }


    /**
     * There is a bug in the code below. May multiple producer threads concurrently generate data on the sink?
     * If yes, how? Find out and fix it.
     */
    @Test
    public void emit_failure() throws InterruptedException {
        //todo: feel free to change code as you need
        Sinks.Many<Integer> sink = Sinks.many().replay().all();

        Thread[] ts = new Thread[50];
        for (int i = 1; i <= 50; i++) {
            int finalI = i;
            Thread thread = new Thread(() -> sink.emitNext(finalI, ((signalType, emitResult) ->
            {
                if (emitResult.isFailure()) {
                    System.out.println("emit failed");
                    return true;
                }
                return false;
            })));
            thread.start();
            ts[i - 1] = thread;
        }
        for (Thread t : ts) {
            t.join();
        }
//        sink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
        sink.emitComplete((s, e) -> true);

        //don't change code below
        StepVerifier.create(sink.asFlux()
                                .doOnNext(System.out::println)
                                .take(50))
                    .expectNextCount(50)
                    .verifyComplete();
    }

    @Test
    public void test_request_with_priority() {

        long start = System.currentTimeMillis();
        AtomicLong onRequest = new AtomicLong();
        StreamingTaskScheduler streamingTaskScheduler = new StreamingTaskScheduler();

        AtomicReference<Disposable> subscribeRef = new AtomicReference<>();
        AtomicBoolean canceled = new AtomicBoolean(false);
        ExecutionControlBase executionControlBase = new ExecutionControlBase();
        Mono<String> someData = Mono.<String>create(sink -> {
            System.out.println("Creating task..." + Thread.currentThread().getName());
            if (canceled.get()) {
                // 已经被取消了, 异步的sink在执行, 但是不用对它做什么
                System.out.println("Task is canceled, no need to execute");
                return;
            }
            System.out.println("Submitting task..."+ Thread.currentThread().getName());
            streamingTaskScheduler.submit(new StreamingTask(
                () -> {
                    System.out.println("Running task..."+ Thread.currentThread().getName());
                    Disposable subscribe = this.asyncAppendToStore("not important")
                        .then(Mono.fromCallable(() -> "some data"))
                        .subscribe(sink::success, sink::error);
                    subscribeRef.set(subscribe);
                    if (canceled.get()) {
                        System.out.println("Task is canceled, dispose it"+ Thread.currentThread().getName());
                        subscribe.dispose();
                    }
                }, 10, canceled,sink::error
            ));
        }).doOnCancel(() -> {
            System.out.println("Canceled");
            canceled.set(true);
            if (subscribeRef.get() != null) {
                System.out.println("Task is canceled, dispose after cancel"+ Thread.currentThread().getName());
                subscribeRef.get().dispose();
            }
        }).doOnRequest((l)-> {onRequest.set(System.currentTimeMillis()); System.out.println("Request: " + l + " " + Thread.currentThread().getName());})
            //.subscribeOn(Schedulers.single());
//            .onErrorContinue()
            .doOnTerminate(() -> System.out.println("On Terminate: " + (System.currentTimeMillis() - onRequest.get()) + " " + Thread.currentThread().getName()))
            .doAfterTerminate(() -> System.out.println("After Terminate: " + (System.currentTimeMillis() - onRequest.get()) + " " + Thread.currentThread().getName()))
        ;
        StepVerifier.create(someData)
//            .thenRequest(1)
                        .expectNext("some data")
//            .thenCancel()
//            .verify()
//            .expectNext("some data")
            .verifyComplete()
        ;
        System.out.println("Time: " + (System.currentTimeMillis() - start));
    }

    @Test
    public void test_request_with_priority_end_In_different_way() throws InterruptedException {

        long start = System.currentTimeMillis();
        AtomicLong onRequest = new AtomicLong();
        StreamingTaskScheduler streamingTaskScheduler = new StreamingTaskScheduler();
        AtomicInteger a = new AtomicInteger(0);

        AtomicReference<Disposable> subscribeRef = new AtomicReference<>();
        AtomicBoolean canceled = new AtomicBoolean(false);
        ExecutionControlBase executionControlBase = new ExecutionControlBase();
        Flux<String> someData = Flux.<String>create(sink -> {
                System.out.println("Creating task..." + Thread.currentThread().getName());
                if (canceled.get()) {
                    // 已经被取消了, 异步的sink在执行, 但是不用对它做什么
                    System.out.println("Task is canceled, no need to execute");
                    return;
                }
                System.out.println("Submitting task..."+ Thread.currentThread().getName());
                streamingTaskScheduler.submit(new StreamingTask(
                    () -> {
                        System.out.println("Running task..."+ Thread.currentThread().getName());
                        Disposable subscribe = this.asyncAppendToStoreFlux("not important")
                            .subscribe(sink::next, sink::error, sink::complete);
                        subscribeRef.set(subscribe);
                        if (canceled.get()) {
                            System.out.println("Task is canceled, dispose it"+ Thread.currentThread().getName());
                            subscribe.dispose();
                        }
                    }, 10, canceled,sink::error
                ));
            })
            .timeout(Duration.ofMillis(500))
            .doOnCancel(() -> {
                System.out.println("Canceled");
                canceled.set(true);
                if (subscribeRef.get() != null) {
                    System.out.println("Task is canceled, dispose after cancel "+ Thread.currentThread().getName());
                    subscribeRef.get().dispose();
                }
            }).doOnRequest((l)-> {onRequest.set(System.currentTimeMillis()); System.out.println("Request: " + l + " " + Thread.currentThread().getName());})
            //.subscribeOn(Schedulers.single());
            //            .onErrorContinue()
            .doOnTerminate(() -> System.out.println("On Terminate: " + (System.currentTimeMillis() - onRequest.get()) + " " + Thread.currentThread().getName()))
            .doAfterTerminate(() -> System.out.println("After Terminate: " + (System.currentTimeMillis() - onRequest.get()) + " " + Thread.currentThread().getName()))
            .doFinally(signalType -> System.out.println("Do Finally: " + signalType + " " + Thread.currentThread().getName()))
            .doOnError(e -> System.out.println("Error: " + e + " " + Thread.currentThread().getName()))


//            .timeout(Duration.ofMillis(500))
            .doOnError((e) -> {
               System.out.println("Downstream Error: " + e + " " + Thread.currentThread().getName());
            })

            // 同时设置两个timeout, 会发生什么? 见下文的解释

        ;

        Disposable subscribe = someData.subscribe();
        System.out.println("Time: " + (System.currentTimeMillis() - start));
        Thread.sleep(5000);
        // 输出
//        Request: 9223372036854775807 main
//        Creating task...main
//        Submitting task...main
//            Canceled
//        Do Finally: cancel parallel-1
//        Time: 64
//            [ERROR] (parallel-1) Operator called default onErrorDropped - reactor.core.Exceptions$ErrorCallbackNotImplemented: java.util.concurrent.TimeoutException: Did not observe any item or terminal signal within 1ms in 'peek' (and no fallback has been configured)
//        reactor.core.Exceptions$ErrorCallbackNotImplemented: java.util.concurrent.TimeoutException: Did not observe any item or terminal signal within 1ms in 'peek' (and no fallback has been configured)
    }

    /**
     * 非常好的观察！让我解释一下这个原理。
     *
     * ## Reactor 操作符链的信号传播原理
     *
     * ### 信号传播方向
     *
     * 在 Reactor 中，不同类型的信号有不同的传播方向：
     *
     * 1. **请求信号 (request)**: 下游 → 上游
     * 2. **数据信号 (onNext, onComplete, onError)**: 上游 → 下游
     * 3. **取消信号 (cancel)**: 下游 → 上游
     *
     * ### timeout 操作符的工作机制
     *
     * ```java
     * // 操作符链的执行顺序
     * Flux.create(...)           // 1. 源
     * .doOnCancel(...)          // 2. 中间操作符
     * .timeout(Duration.ofMillis(10))  // 3. timeout操作符
     * ```
     *
     * 当 timeout 操作符检测到超时时：
     *
     * 1. **timeout 在下游位置**（如 c8_Sinks）:
     *    ```java
     *    .doOnCancel(() -> {...})  // 上游
     *    .timeout(Duration.ofMillis(10))  // 下游
     *    ```
     *    - timeout 向**上游发送 cancel 信号**
     *    - 上游的 [doOnCancel](file://exercises/src/test/java/c8_Sinks.java#283#11) 接收到 cancel 信号并执行
     *    - 输出: `"Do Finally: cancel"`
     *
     * 2. **timeout 在上游位置**（如 c9_ExecutionControl）:
     *    ```java
     *    .timeout(Duration.ofMillis(500))  // 上游
     *    .doOnCancel(() -> {...})  // 下游
     *    ```
     *    - timeout 向**下游发送 error 信号** (TimeoutException)
     *    - 下游的 [doOnCancel](file://exercises/src/test/java/c8_Sinks.java#283#11) 不会被调用，因为这不是 cancel 信号
     *    - 输出: `"signalType: onError"`
     *
     * ## 双重 timeout 的情况
     *
     * 当你同时指定两个 timeout 时：
     *
     * ```java:exercises/src/test/java/c8_Sinks.java
     * .timeout(Duration.ofMillis(500))  // 第一个timeout
     * .doOnCancel(() -> {...})
     * .timeout(Duration.ofMillis(500))  // 第二个timeout
     * ```
     *
     * 执行顺序：
     * 1. 第一个 timeout 超时 → 发送 **error 信号**到下游
     * 2. error 信号比 cancel 信号**优先级更高**
     * 3. 第二个 timeout 接收到 error 信号，直接传播，不会发送 cancel 信号
     * 4. 最终结果：`"Do Finally: onError"`
     *
     * ## 信号优先级
     *
     * 在 Reactor 中，信号的优先级是：
     * ```
     * onError > onComplete > cancel
     * ```
     *
     * 一旦 error 信号开始传播，它会**覆盖**其他类型的终止信号。
     *
     * ## 验证原理的代码示例
     *
     * ```java
     * // 验证信号传播方向
     * Flux.create(sink -> {
     *     // 不发出任何数据
     * })
     * .doOnCancel(() -> System.out.println("上游 doOnCancel"))
     * .timeout(Duration.ofMillis(100))  // timeout在下游
     * .doOnCancel(() -> System.out.println("下游 doOnCancel"))
     * ```
     *
     * 结果：只有"上游 doOnCancel"会被打印，因为 cancel 信号是向上游传播的。
     *
     * 这就是为什么 timeout 操作符的**位置决定了信号类型和传播方向**的根本原理！
     */




    public Flux<String> asyncAppendToStoreFlux(String eventJson) {
        return Flux.<String>create(s -> {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                s.next(eventJson);
                s.next(eventJson);
                s.next(eventJson);
                s.next(eventJson);
                s.next(eventJson);
                s.complete();
             })
            .publishOn(Schedulers.boundedElastic())
            .doOnNext(s -> {System.out.println("Appending event running in: " + Thread.currentThread().getName());})
            .delayElements(Duration.ofMillis(10))
            .doOnCancel(() -> {
                System.out.println("producer canceled");
            })
            .doOnNext(s -> System.out.println("Appending event to store"));

    }

    public Mono<Void> asyncAppendToStore(String eventJson) {
        return Mono.just(eventJson)
            .publishOn(Schedulers.boundedElastic())
            .doOnNext(s -> {System.out.println("Appending event running in: " + Thread.currentThread().getName());})
            .delayElement(Duration.ofMillis(500))
            .doOnNext(s -> System.out.println("Appending event to store"))
            .then();
    }

}
