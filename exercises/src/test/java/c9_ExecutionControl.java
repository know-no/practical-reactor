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

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
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
        }finally {
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
        Flux<String> notifications = readNotifications()
                .doOnNext(System.out::println)
//            .take(Duration.ofSeconds(1)) // take will restrict total time
//            .subscribeOn(Schedulers.boundedElastic())
            .delayElements(Duration.ofMillis(1000), Schedulers.boundedElastic())
                //todo: change this line only
                ;

        StepVerifier.create(notifications
                                    .doOnNext(s -> assertThread(threadId)))
                    .expectNextCount(5)
                    .verifyComplete();
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
        Flux<String> tasks = tasks()
                .flatMapSequential(Function.identity()).doOnNext(i -> {
                    // 会发生在一些parallel的线程上, 因为mono用了delayElement, 它会调度到parallel池
                    System.out.println("next i " + i + " " + Thread.currentThread().getName() );
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

        StepVerifier.create(task)
                    .verifyComplete();
    }

    /**
     * Make task run on thread suited for long, blocking, parallelized work.
     * Answer:
     * - What BlockHound for?
     */
    @Test
    public void blocking() {
        BlockHound.install(); //don't change this line

        Mono<Void> task = Mono.fromRunnable(ExecutionControlBase::blockingCall)
                              .subscribeOn(Schedulers.boundedElastic())//todo: change this line only
                              .then();

        StepVerifier.create(task)
                    .verifyComplete();
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
        Mono<Void> task = Mono.fromRunnable(ExecutionControlBase::blockingCall)
            .subscribeOn(Schedulers.parallel()).then();

        Flux<Void> taskQueue = Flux.just(task, task, task)
//            .publishOn(Schedulers.boundedElastic())
            .flatMap(m -> {
                                    System.out.println("flatMap execute in: " + Thread.currentThread().getName());
//                                    return m;} ); //.subscribeOn(Schedulers.boundedElastic())
                                    return m;}, 3); //.subscribeOn(Schedulers.boundedElastic())
//                                    return m;}, 3).subscribeOn(Schedulers.boundedElastic())
                    ;
//        Mono<Void> task = Mono.fromRunnable(ExecutionControlBase::blockingCall);
//
//        ParallelFlux<Void> taskQueue = Flux.just(task, task, task)
//            .parallel(3)  // 设置并行度为3
//            .runOn(Schedulers.boundedElastic())  // 在 boundedElastic 上运行
//            .flatMap(Function.identity());  // 执行任务

        //don't change code below
        Duration duration = StepVerifier.create(taskQueue)
                                        .expectComplete()
                                        .verify();

        Assertions.assertTrue(duration.getSeconds() <= 2, "Expected to complete in less than 2 seconds");

        Flux<Integer> integerFlux = Flux.range(1, 3).flatMap(i -> Mono.just(i).subscribeOn(Schedulers.parallel()), 3)
            .doOnNext(System.out::println);
        StepVerifier.create(integerFlux)
            .expectNext(1).expectNext(2).expectNext(3).verifyComplete();

        Flux<Integer> integerFluxP = Flux.range(1, 3).flatMap(i -> Mono.just(i).subscribeOn(Schedulers.parallel()), 3)
            .doOnNext(System.out::println).subscribeOn(Schedulers.parallel());
        StepVerifier.create(integerFluxP)
            .expectNext(1).expectNext(2).expectNext(3).verifyComplete();


        Flux<Integer> integerFluxPP =Flux.range(1, 3)
            .flatMapSequential(i -> Mono.fromCallable(() -> {
                Thread.sleep(new Random().nextInt(1000));  // 模拟异步操作
                return i;
            }).subscribeOn(Schedulers.parallel()), 3)
            .doOnNext(System.out::println);
        StepVerifier.create(integerFluxPP)
            .expectNext(1).expectNext(2).expectNext(3).verifyComplete();
    }

    /**
     * Adapt the code so tasks are executed in parallel, but task results should preserve order in which they are invoked.
     */
    @Test
    public void sequential_free_runners() {
        //todo: feel free to change code as you need
        Flux<String> tasks = tasks()
                .flatMapSequential(Function.identity());
        ;

        //don't change code below
        Duration duration = StepVerifier.create(tasks)
                                        .expectNext("1")
                                        .expectNext("2")
                                        .expectNext("3")
                                        .verifyComplete();

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
        Flux<String> eventStream = eventProcessor()
            .parallel().runOn(Schedulers.parallel())
                .filter(event -> !event.metaData.isEmpty())
                .doOnNext(event -> System.out.println("Mapping event: " + event.metaData))
                .map(this::toJson)
            .sequential()
                .concatMap(n -> appendToStore(n).thenReturn(n));

        //don't change code below
        StepVerifier.create(eventStream)
                    .expectNextCount(250)
                    .verifyComplete();

        List<String> steps = Scannable.from(eventStream)
                                      .parents()
                                      .map(Object::toString)
                                      .collect(Collectors.toList());

        String last = Scannable.from(eventStream)
                               .steps()
                               .collect(Collectors.toCollection(LinkedList::new))
                               .getLast();

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
}
