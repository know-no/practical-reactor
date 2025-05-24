import org.junit.jupiter.api.*;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Another way of controlling amount of data flowing is batching.
 * Reactor provides three batching strategies: grouping, windowing, and buffering.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#advanced-three-sorts-batching
 * https://projectreactor.io/docs/core/release/reference/#which.window
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c11_Batching extends BatchingBase {


    @Test
    public void test_group_by() {
        StepVerifier.create(
                Flux.just(1, 3, 5, 2, 4, 6, 11, 12, 13)
                    .groupBy(i -> i % 2 == 0 ? "even" : "odd")
                    .concatMap(g -> g.defaultIfEmpty(-1) //if empty groups, show them
                        .map(String::valueOf) //map to string
                        .startWith(g.key())) //start with the group's key
            )
            .expectNext("odd", "1", "3", "5", "11", "13")
            .expectNext("even", "2", "4", "6", "12")
            .verifyComplete();
    }

    /**
     * To optimize disk writing, write data in batches of max 10 items, per batch.
     */
    @Test
    public void batch_writer() {
        //todo do your changes here
        Flux<Void> dataStream = null;
        Flux<List<Byte>> buffer10 = dataStream().buffer(10);
        Flux<Void> voidFlux = buffer10.flatMap(this::writeToDisk);
        dataStream = voidFlux;

        //do not change the code below
        StepVerifier.create(dataStream)
                    .verifyComplete();

        Assertions.assertEquals(10, diskCounter.get());
    }

    /**
     * You are implementing a command gateway in CQRS based system. Each command belongs to an aggregate and has `aggregateId`.
     * All commands that belong to the same aggregate needs to be sent sequentially, after previous command was sent, to
     * prevent aggregate concurrency issue.
     * But commands that belong to different aggregates can and should be sent in parallel.
     * Implement this behaviour by using `GroupedFlux`, and knowledge gained from the previous exercises.
     */
    @Test
    public void command_gateway() {
        //todo: implement your changes here
        Flux<Void> processCommands = null;
        Flux<GroupedFlux<String, Command>> groupedFluxFlux = inputCommandStream().groupBy(Command::getAggregateId);

        processCommands = groupedFluxFlux.flatMap(g -> g.concatMap(this::sendCommand)).subscribeOn(Schedulers.parallel());

        //do not change the code below
        Duration duration = StepVerifier.create(processCommands)
                .verifyComplete();

        Assertions.assertTrue(duration.getSeconds() <= 3, "Expected to complete in less than 3 seconds");
    }


    /**
     * You are implementing time-series database. You need to implement `sum over time` operator. Calculate sum of all
     * metric readings that have been published during one second.
     */
    @Test
    public void sum_over_time() {
        AtomicLong   counter = new AtomicLong();
        Flux<Long> metrics = metrics()
                //todo: implement your changes here
            .window(Duration.ofSeconds(1))  // 创建1秒的时间窗口
            .flatMap(w -> w.reduce(0L, Long::sum))
            .take(10);

        StepVerifier.create(metrics)
                    .expectNext(45L, 165L, 255L, 396L, 465L, 627L, 675L, 858L, 885L, 1089L)
                    .verifyComplete();
    }
}
