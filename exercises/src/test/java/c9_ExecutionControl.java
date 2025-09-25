import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import reactor.blockhound.BlockHound;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.NonBlocking;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * With multi-core architectures being a commodity nowadays, being able to easily parallelize work is important.
 * Reactor helps with that by providing many mechanisms to execute work in parallel.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#schedulers
 * https://projectreactor.io/docs/core/release/reference/#advanced-parallelizing-parralelflux
 * https://projectreactor.io/docs/core/release/reference/#_the_publishon_method
 * https://projectreactor.io/docs/core/release/reference/#_the_subscribeon_method
 * https://projectreactor.io/docs/core/release/reference/#which.time
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c9_ExecutionControl extends ExecutionControlBase {

    @Test
    public void testFlaMapSeq_and_parallel() throws InterruptedException {
        Flux<String> stringFlux = Flux.fromArray(new Integer[] { 3, 2, 1, 4, 5, 6, 7, 9, 8, 10 }).publishOn(Schedulers.boundedElastic())
            //            .flatMapSequential(i -> Mono.deferContextual(ctx -> {
            .flatMap(
                i -> Mono.deferContextual(ctx -> {
                    //                        System.out.println("i: " + i + " running in: " + Thread.currentThread().getName());
                    //                        try {
                    //                            Thread.sleep(i * 1000);
                    //                        } catch (InterruptedException e) {
                    //                            throw new RuntimeException(e);
                    //                        }
                    StringBuffer name = (StringBuffer) ctx.get("name");
                    name.append(i);
                    return Mono.just(name.toString());
                }).subscribeOn(Schedulers.parallel()), 1 // 理论上flatmap使用并发数为1, 哪怕Mono在额外的线程上运行也会是串行. concatMap == flatMap(mapper, 1)
            ).contextWrite(context -> context.put("name", new StringBuffer()));
        //内部的mono 都被转化成异步的订阅了; 否则都应该订阅在原本的线程上

        stringFlux.subscribe(System.out::println);
        Thread.sleep(15000);
    }

    @Test
    public void testFlaMapSeq_and_context() throws InterruptedException {
        Scheduler scheduler = Schedulers.fromExecutor(Executors.newFixedThreadPool(
            10, new ThreadFactory() {
                AtomicInteger i = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    int i1 = i.incrementAndGet();
                    return new Thread(() -> {
                        System.out.println("突然的" + i1);
                        r.run();
                    });
                }
            }
        ));
        Flux<String> stringFlux = Flux.fromArray(new Integer[] { 3, 2, 1 }).publishOn(Schedulers.boundedElastic()).flatMap(
                x -> {

                    System.out.println("N: " + x + " running in: " + Thread.currentThread().getName());
//                    return Mono.defer(() -> {
//                            //            .flatMapSequential(x -> Mono.defer(() -> {
//                            System.out.println("x: " + x + " running in: " + Thread.currentThread().getName());
//                            try {
//                                Thread.sleep(x * 1000);
//                            } catch (InterruptedException e) {
//                                throw new RuntimeException(e);
//                            }
//
//                            return Mono.just(x);
//                        })
                                            return Mono.just(Thread.currentThread().getName())
                                                ;
//                                                .subscribeOn(scheduler);
//                        .subscribeOn(Schedulers.newParallel("heihei"));
                    //                        .publishOn(Schedulers.newParallel("heihei"));
                }
            )
            .publishOn(Schedulers.newParallel("happy"))
            .doOnNext((i) -> {
                System.out.println("doOnNext: " + i + " " + Thread.currentThread().getName());
            })
            .flatMapSequential(i -> Mono.deferContextual(ctx -> {
                    System.out.println("i: " + i + " running in: " + Thread.currentThread().getName());
                    try {
                        Thread.sleep(1 * 1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    StringBuffer name = (StringBuffer) ctx.get("name");
                    name.append(i);
                    return Mono.just(name.toString());
                })

//                .subscribeOn(Schedulers.parallel())
                )
            .contextWrite(context -> context.put("name", new StringBuffer()));
        //内部的mono 都被转化成异步的订阅了; 否则都应该订阅在原本的线程上

        stringFlux.subscribe(System.out::println);
        Thread.sleep(10000);

        //        stringFlux = Flux.fromArray(new Integer[] { 3, 2, 1 }).(i -> Mono.deferContextual(ctx -> {
        //            System.out.println("i: " + i + " running in: " + Thread.currentThread().getName());
        //            try {
        //                Thread.sleep(i * 1000);
        //            } catch (InterruptedException e) {
        //                throw new RuntimeException(e);
        //            }
        //            StringBuffer name = (StringBuffer) ctx.get("name");
        //            name.append(i);
        //            return Mono.just(name.toString());
        //        })).contextWrite(context -> context.put("name", new StringBuffer()));c
        //
    }

    @Test
    public void testCancelTiming() throws IOException {
        AtomicInteger counter = new AtomicInteger(0);

        Flux<String> flux = Flux.<String>create(sink -> {
                sink.onDispose(() -> {
                    int order = counter.incrementAndGet();
                    System.out.println("sink.onDispose 执行顺序: " + order);
                });
                sink.onCancel(() -> {
                    int order = counter.incrementAndGet();
                    System.out.println("sink.onCancel 执行顺序: " + order);
                });
            })
            //            .timeout(Duration.ofMillis(500))
            .doOnComplete(() -> {
                int order = counter.incrementAndGet();
                System.out.println("doOnComplete 执行顺序: " + order);
            }).doOnCancel(() -> {
                int order = counter.incrementAndGet();
                System.out.println("doOnCancel 执行顺序: " + order);
            }).doFinally((signalType -> {
                int order = counter.incrementAndGet();
                System.out.println("signalType: " + signalType + " 执行顺序: " + order);
            }));

        Disposable subscription = flux.subscribe();
        // 主动取消
        subscription.dispose();

        System.in.read();
        // 输出：
        //        doOnCancel 执行顺序: 1
        //        sink.onCancel 执行顺序: 2
        //        sink.onDispose 执行顺序: 3
        //        signalType: cancel 执行顺序: 4
    }

    // 超时算子放置的位置不同, 也会影响是否调用doOnCancel
    @Test
    public void testTimeoutSig() throws IOException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);

        Flux<String> flux = Flux.<String>create(sink -> {
                sink.onDispose(() -> {
                    int order = counter.incrementAndGet();
                    System.out.println("sink.onDispose 执行顺序: " + order);
                });
                sink.onCancel(() -> {
                    int order = counter.incrementAndGet();
                    System.out.println("sink.onCancel 执行顺序: " + order);
                });
            })
            // 超时 , 不会调用下游doOnCancel,会向下发送error,向上发送cancel
            .log("source").timeout(Duration.ofMillis(500)).log("after-timeout").doOnCancel(() -> {
                int order = counter.incrementAndGet();
                System.out.println("doOnCancel 执行顺序: " + order);
            }).doOnComplete(() -> {
                int order = counter.incrementAndGet();
                System.out.println("doOnComplete 执行顺序: " + order);
            }).doOnTerminate(() -> {
                int order = counter.incrementAndGet();
                System.out.println("terminate: " + " 执行顺序: " + order);
            })
            //            .doOnTerminate(() -> )
            .doFinally((signalType -> {
                int order = counter.incrementAndGet();
                System.out.println("signalType: " + signalType + " 执行顺序: " + order);
            }))
            // 超时 , 会调用doOnCancel; 向上发送cancel, 向下发送error
            //            .timeout(Duration.ofMillis(500))

            ;
        //        timeout操作符的位置决定了信号传播方向：
        //        timeout在downstream → cancel信号向upstream传播 → upstream的doOnCancel被调用
        //        timeout在upstream → error信号向downstream传播 → downstream的doOnCancel不被调用

        Disposable subscription = flux.subscribe();
        // 等超时

        //        System.in.read();
        Thread.sleep(2000);
        System.out.println("sleeped");
        //        subscription.dispose(); 因为已经超时异常了, 取消不会有效果
        Thread.sleep(2000);

        // 输出：
        // sink.onCancel 执行顺序: 1
        // sink.onDispose 执行顺序: 2
        //        terminate:  执行顺序: 3
        // 错误的堆栈打印
        // signalType: onError 执行顺序: 4
        // 如果timeout放在了前面,  doOnCancel 是不会执行的, 也就是说在doOnCancel里是无法感知到一些其他情况的!
        // 但是放在后面就可以, 且终止信号是timeout
    }

    @Test
    public void testEarlyTake() throws IOException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);

        Flux<String> flux = Flux.<String>create(sink -> {
                sink.onDispose(() -> {
                    int order = counter.incrementAndGet();
                    System.out.println("sink.onDispose 执行顺序: " + order);
                });
                sink.onCancel(() -> {
                    int order = counter.incrementAndGet();
                    System.out.println("sink.onCancel 执行顺序: " + order);
                });
                sink.next("1");
                System.out.println("sink emit 1");
                sink.next("2");
                System.out.println("sink emit 2");
                sink.next("3");
                System.out.println("sink emit 3");
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("sleeped and complete");
                System.out.println("sink emit 4");
                sink.complete();
            }).doOnCancel(() -> {
                int order = counter.incrementAndGet();
                System.out.println("doOnCancel before take执行顺序: " + order);
            }).take(2).doOnCancel(() -> {
                int order = counter.incrementAndGet();
                System.out.println("doOnCancel after take执行顺序: " + order);
            }).doOnComplete(() -> {
                int order = counter.incrementAndGet();
                System.out.println("doOnComplete 执行顺序: " + order);
            }).doOnTerminate(() -> {
                int order = counter.incrementAndGet();
                System.out.println("terminate: " + " 执行顺序: " + order);
            })
            //            .doOnTerminate(() -> )
            .doFinally((signalType -> {
                int order = counter.incrementAndGet();
                System.out.println("signalType: " + signalType + " 执行顺序: " + order);
            }));

        Disposable subscription = flux
            // 这里delay 多久和不delay, take1和take x 还有 sink emit的市场都会影响流的结束
            //            .delayElements(Duration.ofMillis(100))
            //            .take(2)
            .subscribe();
        // 等超时

        Thread.sleep(3000);
        // 已经被take了, 取消了,  dispose不会再执行一次
        subscription.dispose();
        Thread.sleep(3000);
        //        sink emit 1
        //        sink emit 2
        //        sink emit 3
        //        doOnCancel 执行顺序: 1
        //        sink.onCancel 执行顺序: 2
        //        sink.onDispose 执行顺序: 3
        //        signalType: cancel 执行顺序: 4
        //        sleeped and complete
        // 或者
        //        sink emit 1
        //        sink emit 2
        //        sink emit 3
        //        sleeped and complete
        //        doOnComplete 执行顺序: 1
        //        terminate:  执行顺序: 2
        //        signalType: onComplete 执行顺序: 3
        //        sink.onDispose 执行顺序: 4
        //        doOnCancel 执行顺序: 5

    }

    @Test
    public void testDownStreamError() throws IOException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);

        Flux<String> flux = Flux.<String>create(sink -> {
                sink.onDispose(() -> {
                    int order = counter.incrementAndGet();
                    System.out.println("sink.onDispose 执行顺序: " + order);
                });
                sink.onCancel(() -> {
                    int order = counter.incrementAndGet();
                    System.out.println("sink.onCancel 执行顺序: " + order);
                });
                sink.next("1");
                System.out.println("sink emit 1");
                sink.next("2");
                System.out.println("sink emit 2");
                sink.next("3");
                System.out.println("sink emit 3");
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("sleeped and complete");
                System.out.println("sink emit 4");
                sink.complete();
            }).doOnCancel(() -> {
                int order = counter.incrementAndGet();
                System.out.println("doOnCancel before take执行顺序: " + order);
            })
            //            .take(3).doOnCancel(() -> {
            //                int order = counter.incrementAndGet();
            //                System.out.println("doOnCancel after take执行顺序: " + order);
            //            })
            .doOnComplete(() -> {
                int order = counter.incrementAndGet();
                System.out.println("doOnComplete 执行顺序: " + order);
            }).doOnTerminate(() -> {
                int order = counter.incrementAndGet();
                System.out.println("terminate: " + " 执行顺序: " + order);
            })
            //            .doOnTerminate(() -> )
            .doFinally((signalType -> {
                int order = counter.incrementAndGet();
                System.out.println("signalType: " + signalType + " 执行顺序: " + order);
            }));

        AtomicInteger a = new AtomicInteger();
        Disposable subscription = flux
            //            .onErrorContinue((e, v) -> {
            //                System.out.println(e);
            //                System.out.println(v);
            //            })
            .map((i) -> {
                if (a.getAndIncrement() > 1) {
                    throw new RuntimeException("xxxxx");
                } else {
                    return i;
                }

            }).onErrorContinue((e, v) -> {
                System.out.println(e);
                System.out.println(v);
            }).doOnNext(i -> System.out.println("doOnNext: " + i))

            .subscribe();
        // 等超时

        Thread.sleep(3000);
        // 已经被take了, 取消了,  dispose不会再执行一次
        subscription.dispose();
        Thread.sleep(3000);
    }

    @Test
    void testConcurrency() {
        AtomicInteger counter = new AtomicInteger();
        Set<String> threadNames = ConcurrentHashMap.newKeySet();

        Flux.range(1, 1000).parallel(4)  // 🔑 强制并发
            .runOn(Schedulers.parallel()).flatMap(i -> {
                threadNames.add(Thread.currentThread().getName());  // 记录线程名
                return Flux.just("item-" + i);
            })
            //            .sequential()
            .subscribe(item -> counter.incrementAndGet());

        // 结果：多个线程名被记录，证明有并发
        System.out.println("Thread count: " + threadNames.size());  // > 1
    }

    @Test
    void testBlockingBehavior() {
        long start = System.currentTimeMillis();

        // 要认真区分这些差别
        Flux.range(1, 5)
            //            .parallel(5)
            //            .runOn(Schedulers.parallel())
            .flatMap(i -> Flux.just(simulateHeavyOperation(i)), 5)  // 🔑 阻塞操作
            .subscribe(System.out::println);

        long duration = System.currentTimeMillis() - start;
        System.out.println("Total time: " + duration + "ms");  // 约 5秒（串行执行）
    }

    private String simulateHeavyOperation(int i) {
        try {
            Thread.sleep(1000);  // 模拟1秒的重操作
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return "Result-" + i;
    }

    @Test
    void testParallelFlatMapOrdering() {
        List<String> results = Collections.synchronizedList(new ArrayList<>());
        Map<String, List<Integer>> threadResults = new ConcurrentHashMap<>();

        List<String> block = Flux.range(1, 20).parallel(4).runOn(Schedulers.parallel()).flatMap(i -> {
            String threadName = Thread.currentThread().getName();
            threadResults.computeIfAbsent(threadName, k -> new ArrayList<>()).add(i);

            // 模拟重操作
            //                try { Thread.sleep(100); } catch (InterruptedException e) {}
            return Flux.just("result-" + i);
        }).sequential().doOnNext(i -> System.out.println(Thread.currentThread().getName())).collectList().block();
        // sequential 只是合并了一个轨道

        // 验证：每个线程内的处理顺序是有序的
        threadResults.forEach((thread, items) -> {
            System.out.println(thread + ": " + items);
            // 每个线程内：[1,5,9,13,17] 或 [2,6,10,14,18] 等 - 有序
        });
        System.out.println(block);
    }

    @Test
    void testFlatMapWithSubscribeOn() {
        System.out.println("Main thread: " + Thread.currentThread().getName());

        Flux.range(1, 3).doOnNext(i -> System.out.println("Upstream: " + i + " on " + Thread.currentThread().getName())).flatMap(i ->
            // 此callable并非是 Flux.just的那个callable
            Mono.fromCallable(() -> {
                System.out.println("Heavy work: " + i + " on " + Thread.currentThread().getName());
                Thread.sleep(1000);
                return "result-" + i;
            }).subscribeOn(Schedulers.boundedElastic())  // 🔑 指定执行线程池
        ).doOnNext(result -> System.out.println("Result: " + result + " on " + Thread.currentThread().getName())).blockLast();
    }

    @Test
    void testFlatMapWithPublishOn() {
        Flux.range(1, 3)
            .doOnNext(i -> System.out.println("Upstream: " + i + " on " + Thread.currentThread().getName()))
            .flatMap(i -> Flux.just("processing-" + i).map(s -> {
                    simulateHeavyOperation(i);
                    System.out.println("Processing: " + s + " on " + Thread.currentThread().getName());
                    return s + "-done";
                }).publishOn(Schedulers.parallel())  // 🔑 切换下游线程池, 后续的结果也会在对应的线程池里
            )
            .doOnNext(result -> System.out.println("Result: " + result + " on " + Thread.currentThread().getName()))
            .blockLast();
    }

    @Test
    void testFlatMapNoScheduler() {
        Flux.range(1, 3)
            .doOnNext(i -> System.out.println("Upstream: " + i + " on " + Thread.currentThread().getName()))
            .flatMap(i -> Flux.range(i * 10, 2)  // 🔑 内部 Flux 无调度器
                .map(n -> {
                    simulateHeavyOperation(i);
                    System.out.println("Inner processing: " + n + " on " + Thread.currentThread().getName());
                    return "result-" + n;
                }))
            .doOnNext(result -> System.out.println("Result: " + result + " on " + Thread.currentThread().getName()))
            .blockLast();
    }

    @Test
    void testFlatMapNoScheduler2() {
        Flux.range(1, 4)
            .doOnNext(i -> System.out.println("Upstream: " + i + " on " + Thread.currentThread().getName()))
            .flatMap(i -> Flux.range(i * 10, 2)  // 🔑 内部 Flux 有调度器
                .map(n -> {
                    System.out.println("Inner processing: " + n + " on " + Thread.currentThread().getName());
                    return "result-" + n;
                }).publishOn(Schedulers.parallel()) // 影响元素后续的处理线程
            )
            .doOnNext(result -> System.out.println("Result: " + result + " on " + Thread.currentThread().getName()))
            .blockLast();
    }

    @Test
    void testThreadPropagation() {
        Flux.range(1, 3).doOnNext(i -> System.out.println("1. Upstream: " + i + " on " + Thread.currentThread().getName())).flatMap(i -> {
            System.out.println("2. FlatMap start: " + i + " on " + Thread.currentThread().getName());
            return Flux.just("processing-" + i)
                .doOnNext(s -> System.out.println("3. Before map: " + s + " on " + Thread.currentThread().getName()))
                .map(s -> {
                    System.out.println("4. Inside map: " + s + " on " + Thread.currentThread().getName());
                    return s + "-done";
                })
                .doOnNext(s -> System.out.println("5. After map: " + s + " on " + Thread.currentThread().getName()))
                .publishOn(Schedulers.parallel())
                .doOnNext(s -> System.out.println("6. After publishOn: " + s + " on " + Thread.currentThread().getName()));
        }).doOnNext(result -> System.out.println("7. Outside flatMap: " + result + " on " + Thread.currentThread().getName())).blockLast();
    }

    @Test
    public void test_flat_map_and_mono_thread() throws InterruptedException {

        ExecutorService ex = Executors.newFixedThreadPool(
            10, new ThreadFactory() {
                AtomicInteger x = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread("thread-x-" + x.getAndIncrement());
                    thread.setDaemon(true);
                    return thread;
                }
            }
        );
        Scheduler schedulerX = Schedulers.fromExecutorService(ex);
        ExecutorService eo = Executors.newFixedThreadPool(
            10, new ThreadFactory() {
                AtomicInteger x = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread("thread-o-" + x.getAndIncrement());
                    thread.setDaemon(true);
                    return thread;
                }
            }
        );
        Scheduler schedulerO = Schedulers.fromExecutorService(eo);
        try {
            Scheduler schedulerAAA = Schedulers.newParallel("myParallel_AAA", 10);
            Scheduler schedulerBBB = Schedulers.newParallel("myParallel_BBB", 10);
            Flux<Integer> originFlux = Flux.range(0, 1000).publishOn(Schedulers.boundedElastic());

            Flux<Integer> afterFlatMapSeq = originFlux.flatMap((i) -> {
                System.out.println("originFlux execute flatMapSeq in: " + Thread.currentThread().getName());
                return Mono.fromSupplier(() -> {
                    System.out.println("mono in originFlux execute in: " + Thread.currentThread().getName());
                    return i;
                }); //.subscribeOn(i % 2 == 0 ? schedulerAAA : schedulerBBB);
            });//.publishOn(schedulerBBB);
            Flux<Integer> finalFlux = afterFlatMapSeq.flatMap((i) -> {
                System.out.println("afterFlatMapSeq execute flatMapSeq in: " + Thread.currentThread().getName());
                return Mono.fromSupplier(() -> {
                    System.out.println("mono in afterFlatmapSeq execute in: " + Thread.currentThread().getName());
                    return i;
                });
            }).doOnNext((i) -> {
                System.out.println("finalFlux execute subscribe in: " + Thread.currentThread().getName());
            }).doOnComplete(() -> {
                System.out.println("Flow completed!");
            });

            StepVerifier.create(finalFlux).thenConsumeWhile(i -> {
                    System.out.println(i);
                    return true;
                })  // 消费所有元素
                .verifyComplete();
        } finally {
            eo.shutdown();
            ex.shutdown();

        }

    }

    /**
     * You are working on smartphone app and this part of code should show user his notifications. Since there could be
     * multiple notifications, for better UX you want to slow down appearance between notifications by 1 second.
     * Pay attention to threading, compare what code prints out before and after solution. Explain why?
     */
    @Test
    public void slow_down_there_buckaroo() {
        long threadId = Thread.currentThread().getId();
        Flux<String> notifications = readNotifications().doOnNext(System.out::println)
            //            .take(Duration.ofSeconds(1)) // take will restrict total time
            //            .subscribeOn(Schedulers.boundedElastic())
            .delayElements(Duration.ofMillis(1000), Schedulers.boundedElastic())
            //todo: change this line only
            ;

        StepVerifier.create(notifications.doOnNext(s -> assertThread(threadId))).expectNextCount(5).verifyComplete();
    }

    private void assertThread(long invokerThreadId) {
        long currentThread = Thread.currentThread().getId();
        if (currentThread != invokerThreadId) {
            System.out.println("-> Not on the same thread");
        } else {
            System.out.println("-> On the same thread");
        }
        Assertions.assertTrue(currentThread != invokerThreadId, "Expected to be on a different thread");
    }

    /**
     * You are using free access to remote hosting machine. You want to execute 3 tasks on this machine, but machine
     * will allow you to execute one task at a time on a given schedule which is orchestrated by the semaphore. If you
     * disrespect schedule, your access will be blocked.
     * Delay execution of tasks until semaphore signals you that you can execute the task.
     */
    @Test
    public void ready_set_go() {
        //todo: feel free to change code as you need
        Flux<String> tasks = tasks().flatMapSequential(Function.identity()).doOnNext(i -> {
            // 会发生在一些parallel的线程上, 因为mono用了delayElement, 它会调度到parallel池
            System.out.println("next i " + i + " " + Thread.currentThread().getName());
        });
        Flux<String> semaphore = semaphore(); // 这里的关键点事tasks是flatMap执行的, 即乱序的;所以得用Sequential
        tasks = tasks.zipWith(semaphore, (t, s) -> t).doOnNext(i -> {
            System.out.println("task: " + i + " " + Thread.currentThread().getName());
        });
        //        zipWith 会在发出元素的线程上执行组合操作
        //        由于 semaphore 是驱动方（因为它的间隔控制了输出节奏）
        //        所以组合后的元素会在 semaphore 的线程上发出
        //
        //        Flux<String> semaphore = semaphore();
        //        Flux<String> tasks = tasks()
        //            .flatMap(task -> {
        //                return task.delaySubscription(semaphore.next());
        //            });

        //don't change code below
        StepVerifier.create(tasks)
            .expectNext("1")
            .expectNoEvent(Duration.ofMillis(2000))
            .expectNext("2")
            .expectNoEvent(Duration.ofMillis(2000))
            .expectNext("3")
            .verifyComplete();
    }

    /**
     * Make task run on thread suited for short, non-blocking, parallelized work.
     * Answer:
     * - Which types of schedulers Reactor provides?
     * - What is their purpose?
     * - What is their difference?
     */
    @Test
    public void non_blocking() {
        Mono<Void> task = Mono.fromRunnable(() -> {
                Thread currentThread = Thread.currentThread();
                assert NonBlocking.class.isAssignableFrom(Thread.currentThread().getClass());
                System.out.println("Task executing on: " + currentThread.getName());
            }).subscribeOn(Schedulers.parallel())
            //todo: change this line only
            .then();

        StepVerifier.create(task).verifyComplete();
    }

    /**
     * Make task run on thread suited for long, blocking, parallelized work.
     * Answer:
     * - What BlockHound for?
     */
    @Test
    public void blocking() {
        BlockHound.install(); //don't change this line

        Mono<Void> task =
            Mono.fromRunnable(ExecutionControlBase::blockingCall).subscribeOn(Schedulers.boundedElastic())//todo: change this line only
                .then();

        StepVerifier.create(task).verifyComplete();
    }

    /**
     * Adapt code so tasks are executed in parallel, with max concurrency of 3.
     */
    @Test
    public void free_runners() {
        //todo: feel free to change code as you need
        // 如下, task被固定在了main线程, 只有一个main线程
        //        Mono<Void> task = Mono.fromRunnable(ExecutionControlBase::blockingCall);
        //
        //        Flux<Void> taskQueue = Flux.just(task, task, task)
        //            .flatMap(m -> m, 3)
        //
        //            ;
        //                                   .flatMap(Function.identity(), 30).subscribeOn(Schedulers.newParallel(",", 3));

        // 这样也不行, 因为subscribeOn只影响到了Flux.just的执行, 它执行在了线程池里, 当他选择了一个线程后
        // 后续的执行也在这个线程哈桑, 而flatMap没有用,因为没有设置线程池, 没有publishOn线程池
        //        Mono<Void> task = Mono.fromRunnable(ExecutionControlBase::blockingCall);
        //////
        //        Flux<Void> taskQueue = Flux.fromStream(() -> {
        //                System.out.println("flux execute in : " + Thread.currentThread().getName());
        //                return Stream.of(task, task, task);
        //            })
        //            .subscribeOn(Schedulers.newParallel("myParallel", 3))
        //            .flatMap(m -> {
        //                System.out.println("flatMap execute in: " + Thread.currentThread().getName());
        //                return m;}, 3)
        //            ;
        Mono<Void> task = Mono.fromRunnable(ExecutionControlBase::blockingCall).subscribeOn(Schedulers.parallel()).then();

        Flux<Void> taskQueue = Flux.just(task, task, task)
            //            .publishOn(Schedulers.boundedElastic())
            .flatMap(
                m -> {
                    System.out.println("flatMap execute in: " + Thread.currentThread().getName());
                    //                                    return m;} ); //.subscribeOn(Schedulers.boundedElastic())
                    return m;
                }, 3
            ); //.subscribeOn(Schedulers.boundedElastic())
        //                                    return m;}, 3).subscribeOn(Schedulers.boundedElastic())
        ;
        //        Mono<Void> task = Mono.fromRunnable(ExecutionControlBase::blockingCall);
        //
        //        ParallelFlux<Void> taskQueue = Flux.just(task, task, task)
        //            .parallel(3)  // 设置并行度为3
        //            .runOn(Schedulers.boundedElastic())  // 在 boundedElastic 上运行
        //            .flatMap(Function.identity());  // 执行任务

        //don't change code below
        Duration duration = StepVerifier.create(taskQueue).expectComplete().verify();

        Assertions.assertTrue(duration.getSeconds() <= 2, "Expected to complete in less than 2 seconds");

        Flux<Integer> integerFlux =
            Flux.range(1, 3).flatMap(i -> Mono.just(i).subscribeOn(Schedulers.parallel()), 3).doOnNext(System.out::println);
        StepVerifier.create(integerFlux).expectNext(1).expectNext(2).expectNext(3).verifyComplete();

        Flux<Integer> integerFluxP = Flux.range(1, 3)
            .flatMap(i -> Mono.just(i).subscribeOn(Schedulers.parallel()), 3)
            .doOnNext(System.out::println)
            .subscribeOn(Schedulers.parallel());
        StepVerifier.create(integerFluxP).expectNext(1).expectNext(2).expectNext(3).verifyComplete();

        Flux<Integer> integerFluxPP = Flux.range(1, 3).flatMapSequential(
            i -> Mono.fromCallable(() -> {
                Thread.sleep(new Random().nextInt(1000));  // 模拟异步操作
                return i;
            }).subscribeOn(Schedulers.parallel()), 3
        ).doOnNext(System.out::println);
        StepVerifier.create(integerFluxPP).expectNext(1).expectNext(2).expectNext(3).verifyComplete();
    }

    /**
     * Adapt the code so tasks are executed in parallel, but task results should preserve order in which they are invoked.
     */
    @Test
    public void sequential_free_runners() {
        //todo: feel free to change code as you need
        Flux<String> tasks = tasks().flatMapSequential(Function.identity());
        ;

        //don't change code below
        Duration duration = StepVerifier.create(tasks).expectNext("1").expectNext("2").expectNext("3").verifyComplete();

        Assertions.assertTrue(duration.getSeconds() <= 1, "Expected to complete in less than 1 seconds");
    }

    /**
     * Make use of ParallelFlux to branch out processing of events in such way that:
     * - filtering events that have metadata, printing out metadata, and mapping to json can be done in parallel.
     * Then branch in before appending events to store. `appendToStore` must be invoked sequentially!
     */
    @Test
    public void event_processor() {
        //todo: feel free to change code as you need
        Flux<String> eventStream = eventProcessor().parallel()
            .runOn(Schedulers.parallel())
            .filter(event -> !event.metaData.isEmpty())
            .doOnNext(event -> System.out.println("Mapping event: " + event.metaData))
            .map(this::toJson)
            .sequential()
            .concatMap(n -> appendToStore(n).thenReturn(n));

        //don't change code below
        StepVerifier.create(eventStream).expectNextCount(250).verifyComplete();

        List<String> steps = Scannable.from(eventStream).parents().map(Object::toString).collect(Collectors.toList());

        String last = Scannable.from(eventStream).steps().collect(Collectors.toCollection(LinkedList::new)).getLast();

        Assertions.assertEquals("concatMap", last);
        Assertions.assertTrue(steps.contains("ParallelMap"), "Map operator not executed in parallel");
        Assertions.assertTrue(steps.contains("ParallelPeek"), "doOnNext operator not executed in parallel");
        Assertions.assertTrue(steps.contains("ParallelFilter"), "filter operator not executed in parallel");
        Assertions.assertTrue(steps.contains("ParallelRunOn"), "runOn operator not used");
    }

    ObjectMapper x = new ObjectMapper();

    private String toJson(Event n) {
        try {
            return x.writeValueAsString(n);
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }

    @Test
    public void test2() throws InterruptedException {
        // 模拟场景
        Flux<String> innerFlux = Flux.just("1", "2", "3").delayElements(Duration.ofMillis(100)).take(1); // 这会在发送第一个元素后取消

        Flux<String> outerFlux = Flux.create(sink -> {
            innerFlux.subscribe(
                sink::next,    // 只会收到 "1"
                sink::error,   // 不会被调用
                sink::complete // 不会被调用，因为是取消而不是完成
            );
        });
        Disposable subscribe = outerFlux.subscribe(System.out::println);

        // 外部订阅者会一直等待，因为 sink 没有完成或出错
        Thread.currentThread().sleep(5000);
        System.out.println(subscribe.isDisposed());

    }
}
