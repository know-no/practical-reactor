import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In this chapter we will learn difference between hot and cold publishers,
 * how to split a publisher into multiple and how to keep history so late subscriber don't miss any updates.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#reactor.hotCold
 * https://projectreactor.io/docs/core/release/reference/#which.multicasting
 * https://projectreactor.io/docs/core/release/reference/#advanced-broadcast-multiple-subscribers-connectableflux
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c12_Broadcasting extends BroadcastingBase {

    /**
     * Split incoming message stream into two streams, one contain user that sent message and second that contains
     * message payload.
     */
    @Test
    public void sharing_is_caring() throws InterruptedException {
        Flux<Message> messages = messageStream()
                //todo: do your changes here
//            .publishOn(Schedulers.parallel(), 16)  // 显式设置缓冲区大小
//            .onBackpressureBuffer(16)
//            .limitRate(16)
//            .publish()
//            .refCount();
            .share()
//            .publish(10).refCount()
                ;

        //don't change code below
        Flux<String> userStream = messages.map(m -> m.user);
        Flux<String> payloadStream = messages.map(m -> m.payload);

        CopyOnWriteArrayList<String> metaData = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<String> payload = new CopyOnWriteArrayList<>();

        // 分别订阅
        userStream.doOnNext(n -> System.out.println("User: " + n)).subscribe(metaData::add);
        payloadStream.doOnNext(n -> System.out.println("Payload: " + n)).subscribe(payload::add);


        Thread.sleep(5000);

        Assertions.assertEquals(Arrays.asList("user#0", "user#1", "user#2", "user#3", "user#4"), metaData);
        Assertions.assertEquals(Arrays.asList("payload#0", "payload#1", "payload#2", "payload#3", "payload#4"),
                                payload);

        // 并发订阅, 并且

        Flux<String> map = messages.map(m -> m.user);
        Flux<String> map1 = messages.map(m -> m.payload);
        // 为什么输出从32起, 因为上面的订阅着完成后, messages被取消订阅.  当新的订阅着来的时候, atomicLong是32, 所以重新生产了32个元素(32-63)
        // 而为什么message会产生32个元素, 而不是take的5个, 是因为prefetch, 并且使用异步generate好像无法控制预取的个数
        Flux<String> stringFlux = map.doOnNext(n -> System.out.println("User: " + n)).doOnSubscribe(System.out::println);
        Flux<String> stringFlux1 = map1.doOnNext(n -> System.out.println("Payload: " + n)).doOnSubscribe(System.out::println);

        new Thread(() -> {
            stringFlux.subscribeOn(Schedulers.parallel()).subscribe();
        }).start();


        new Thread(() -> {
            stringFlux1.subscribeOn(Schedulers.parallel()).subscribe();
        }).start();


        Thread.sleep(6000);
    }

    /**
     * Since two subscribers are interested in the updates, which are coming from same source, convert `updates` stream
     * to from cold to hot source.
     * Answer: What is the difference between hot and cold publisher? Why does won't .share() work in this case?
     */
    @Test
    public void hot_vs_cold() throws InterruptedException {
        Flux<String> updates = systemUpdates().publish().autoConnect();
                //todo: do your changes here
                ;

        //subscriber 1
        StepVerifier.create(updates.take(3).doOnNext(n -> System.out.println("subscriber 1 got: " + n)))
                    .expectNext("RESTARTED", "UNHEALTHY", "HEALTHY")
                    .verifyComplete();

        //subscriber 2
        StepVerifier.create(updates.take(4).doOnNext(n -> System.out.println("subscriber 2 got: " + n)))
                    .expectNext("DISK_SPACE_LOW", "OOM_DETECTED", "CRASHED", "UNKNOWN")
                    .verifyComplete();

        updates = systemUpdates().publish().autoConnect(2);

        // 创建两个子流
        Flux<String> stream1 = updates.take(3);
        Flux<String> stream2 = updates.take(4);

        // 同时订阅两个流

        Runnable runnable = () -> {
            StepVerifier.create(stream1.doOnNext(n -> System.out.println("stream1 got: " + n)))
                .expectNext("RESTARTED", "UNHEALTHY", "HEALTHY")
                .verifyComplete();
        };

        Runnable runnable1 = () -> {
            StepVerifier.create(stream2.doOnNext(n -> System.out.println("stream2 got: " + n)))
                .expectNext("RESTARTED", "UNHEALTHY", "HEALTHY", "DISK_SPACE_LOW")
                .verifyComplete();
        };

        Thread thread1 = new Thread(runnable);
        Thread thread2 = new Thread(runnable1);
        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();



        updates = systemUpdates().publish().autoConnect();

        // 创建两个子流
        Flux<String> stream3 = updates.take(3);
        Flux<String> stream4 = updates.take(4);

        // 同时订阅两个流

        Runnable runnable3 = () -> {
            StepVerifier.create(stream3.doOnNext(n -> System.out.println("stream3 got: " + n)))
                .expectNext("RESTARTED", "UNHEALTHY", "HEALTHY")
                .verifyComplete();
        };

        Runnable runnable4 = () -> {
            StepVerifier.create(stream4.doOnNext(n -> System.out.println("stream4 got: " + n)))
                .expectNext("RESTARTED", "UNHEALTHY", "HEALTHY", "DISK_SPACE_LOW")
                .verifyComplete();
        };

        Thread thread3 = new Thread(runnable3);
        Thread thread4 = new Thread(runnable4);
        thread3.start();
        Thread.sleep(1500); // 确保thread3先执行, stream4接收到第一个元素是UNHEALTHY
        thread4.start();

        thread3.join();
        thread4.join();

    }

    /**
     * In previous exercise second subscriber subscribed to update later, and it missed some updates. Adapt previous
     * solution so second subscriber will get all updates, even the one's that were broadcaster before its
     * subscription.
     */
    @Test
    public void history_lesson() {
        // 使用cache, 而不是share, 因为 share == publish().refCount(), 虽然二者都是在第一个订阅着到达时候开始发送数据
        // 但是前者即使所有订阅者都取消订阅了, 数据流也会继续. 不会重新闯将数据源. 而后者, 当所有订阅者都取消订阅时 ,会去取消上游数据源的订阅,当新的订阅者到达时，会重新订阅上游数据源
        Flux<String> updates = systemUpdates().cache()
                //todo: do your changes here
                ;

        //subscriber 1
        StepVerifier.create(updates.take(3).doOnNext(n -> System.out.println("subscriber 1 got: " + n)))
                    .expectNext("RESTARTED", "UNHEALTHY", "HEALTHY")
                    .verifyComplete();

        //subscriber 2
        StepVerifier.create(updates.take(5).doOnNext(n -> System.out.println("subscriber 2 got: " + n)))
                    .expectNext("RESTARTED", "UNHEALTHY", "HEALTHY", "DISK_SPACE_LOW", "OOM_DETECTED")
                    .verifyComplete();
    }

    @Test
    public void cache_behavior() {
        Flux<String> cachedFlux = systemUpdates().cache();

        // 第一个订阅者
        StepVerifier.create(cachedFlux.take(2))
            .expectNext("RESTARTED", "UNHEALTHY")
            .verifyComplete();

        // 第二个订阅者会收到所有历史数据
        StepVerifier.create(cachedFlux.take(3))
            .expectNext("RESTARTED", "UNHEALTHY", "HEALTHY")
            .verifyComplete();
    }

    @Test
    public void concurrent_subscription() throws InterruptedException {
        Flux<String> sharedFlux = systemUpdates().share();

        // 创建两个并发订阅者
        Runnable sharedSubscriber1 = () -> {
            StepVerifier.create(sharedFlux.take(2))
                .expectNext("RESTARTED", "UNHEALTHY")
                .verifyComplete();
        };

        Runnable sharedSubscriber2 = () -> {
            StepVerifier.create(sharedFlux.take(2))
                .expectNext("RESTARTED", "UNHEALTHY")
                .verifyComplete();
        };

        // 并发执行
        Thread t1 = new Thread(sharedSubscriber1);
        Thread t2 = new Thread(sharedSubscriber2);
        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }



}
