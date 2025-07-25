import org.junit.jupiter.api.*;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * In this important chapter we are going to cover different ways of combining publishers.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#which.values
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c6_CombiningPublishers extends CombiningPublishersBase {

    @Test
    public void pick_concatmap_next() {
        Flux<Object> flux = Flux.create(sink -> {
            sink.next("one");
            sink.next("two");
            sink.complete();
        }).concatMap(s -> {
            if (s.equals("one")) {
                return Mono.empty();
            } else {
                return Mono.just(s);
            }
        });
        flux.next().subscribe(System.out::println);

        Flux.generate(
            () -> 0, (state, sink) -> {
                System.out.println("state is: " + state );
                sink.next(state);
                return state + 1;
            }
        ).concatMap(d -> Mono.justOrEmpty(Objects.equals(d, 2) ? d:null)).next().subscribe(System.out::println);

}

/**
 * Goal of this exercise is to retrieve e-mail of currently logged-in user.
 * `getCurrentUser()` method retrieves currently logged-in user
 * and `getUserEmail()` will return e-mail for given user.
 *
 * No blocking operators, no subscribe operator!
 * You may only use `flatMap()` operator.
 */
@Test
public void behold_flatmap() {
    Hooks.enableContextLossTracking(); //used for testing - detects if you are cheating!

    //todo: feel free to change code as you need
    Mono<String> currentUserEmail = null;
    Mono<String> currentUserMono = getCurrentUser();
    currentUserEmail = currentUserMono.flatMap(user -> getUserEmail(user));

    //don't change below this line
    StepVerifier.create(currentUserEmail).expectNext("user123@gmail.com").verifyComplete();
}

/**
 * `taskExecutor()` returns tasks that should execute important work.
 * Get all the tasks and execute them.
 *
 * Answer:
 * - Is there a difference between Mono.flatMap() and Flux.flatMap()?
 */
@Test
public void task_executor() {
    //todo: feel free to change code as you need
    Flux<Void> tasks = null;
    Flux<Void> voidFlux = taskExecutor().flatMap(Function.identity());
    tasks = voidFlux;

    //don't change below this line
    StepVerifier.create(tasks).verifyComplete();

    Assertions.assertEquals(taskCounter.get(), 10);
}

/**
 * `streamingService()` opens a connection to the data provider.
 * Once connection is established you will be able to collect messages from stream.
 *
 * Establish connection and get all messages from data provider stream!
 */
@Test
public void streaming_service() {
    //todo: feel free to change code as you need
    Flux<Message> messageFlux = null;
    Flux<Message> messageFlux1 = streamingService().flatMapMany(Function.identity());
    messageFlux = messageFlux1;

    //don't change below this line
    StepVerifier.create(messageFlux).expectNextCount(10).verifyComplete();
}

/**
 * Join results from services `numberService1()` and `numberService2()` end-to-end.
 * First `numberService1` emits elements and then `numberService2`. (no interleaving)
 *
 * Bonus: There are two ways to do this, check out both!
 */
@Test
public void i_am_rubber_you_are_glue() {
    //todo: feel free to change code as you need
    Flux<Integer> numbers = null;
    Flux<Integer> n1 = numberService1();
    Flux<Integer> n2 = numberService2();
    numbers = n1.concatWith(n2);
    //        numbers = n1.mergeWith(n2);

    //don't change below this line
    StepVerifier.create(numbers).expectNext(1, 2, 3, 4, 5, 6, 7).verifyComplete();
}

/**
 * Similar to previous task:
 *
 * `taskExecutor()` returns tasks that should execute important work.
 * Get all the tasks and execute each of them.
 *
 * Instead of flatMap() use concatMap() operator.
 *
 * Answer:
 * - What is difference between concatMap() and flatMap()?
 * - What is difference between concatMap() and flatMapSequential()?
 * - Why doesn't Mono have concatMap() operator?
 */
@Test
public void task_executor_again() {
    //todo: feel free to change code as you need
    Flux<Void> tasks = null;
    tasks = taskExecutor().concatMap(Mono::flux);

    //don't change below this line
    StepVerifier.create(tasks).verifyComplete();

    Assertions.assertEquals(taskCounter.get(), 10);
}

/**
 * You are writing software for broker house. You can retrieve current stock prices by calling either
 * `getStocksGrpc()` or `getStocksRest()`.
 * Since goal is the best response time, invoke both services but use result only from the one that responds first.
 */
@Test
public void need_for_speed() {
    //todo: feel free to change code as you need
    Flux<String> stonks = null;
    Flux<String> f1 = getStocksGrpc();
    Flux<String> f2 = getStocksRest();
    stonks = Flux.firstWithSignal(f1, f2);

    //don't change below this line
    StepVerifier.create(stonks).expectNextCount(5).verifyComplete();
}

/**
 * As part of your job as software engineer for broker house, you have also introduced quick local cache to retrieve
 * stocks from. But cache may not be formed yet or is empty. If cache is empty, switch to a live source:
 * `getStocksRest()`.
 */
@Test
public void plan_b() {
    //todo: feel free to change code as you need
    Flux<String> stonks = null;
    Flux<String> stocksLocalCache = getStocksLocalCache();
    Flux<String> stocksRest = getStocksRest();
    stonks = stocksLocalCache.switchIfEmpty(stocksRest);

    //don't change below this line
    StepVerifier.create(stonks).expectNextCount(6).verifyComplete();

    Assertions.assertTrue(localCacheCalled.get());
}

/**
 * You are checking mail in your mailboxes. Check first mailbox, and if first message contains spam immediately
 * switch to a second mailbox. Otherwise, read all messages from first mailbox.
 */
@Test
public void mail_box_switcher() {
    //todo: feel free to change code as you need
    Flux<Message> myMail = null;
    Flux<Message> m1 = mailBoxPrimary();
    Flux<Message> m2 = mailBoxSecondary();

    myMail = m1.switchOnFirst((sig, flux) -> {
        if (Objects.equals(sig.get().metaData, "spam")) {
            return m2;
        } else {
            return flux;
        }
    });

    //don't change below this line
    StepVerifier.create(myMail)
        .expectNextMatches(m -> !m.metaData.equals("spam"))
        .expectNextMatches(m -> !m.metaData.equals("spam"))
        .verifyComplete();

    Assertions.assertEquals(1, consumedSpamCounter.get());
}

/**
 * You are implementing instant search for software company.
 * When user types in a text box results should appear in near real-time with each keystroke.
 *
 * Call `autoComplete()` function for each user input
 * but if newer input arrives, cancel previous `autoComplete()` call and call it for latest input.
 */
@Test
public void instant_search() {
    //todo: feel free to change code as you need
    //        autoComplete(null);
    Flux<String> suggestions = userSearchInput().switchMap(this::autoComplete)
        //todo: use one operator only
        ;

    //don't change below this line
    StepVerifier.create(suggestions).expectNext("reactor project", "reactive project").verifyComplete();
}

/**
 * Code should work, but it should also be easy to read and understand.
 * Orchestrate file writing operations in a self-explanatory way using operators like `when`,`and`,`then`...
 * If all operations have been executed successfully return boolean value `true`.
 */
@Test
public void prettify() {
    //todo: feel free to change code as you need
    //todo: use when,and,then...
    Mono<Boolean> successful = null;

    Mono<Void> v1 = openFile();
    Mono<Void> v2 = writeToFile("0x3522285912341");
    Mono<Void> v3 = closeFile();

    successful = v1.and(v2).and(v3).thenReturn(true);
    successful = Mono.when(v1, v2, v3).thenReturn(true);
    //don't change below this line
    StepVerifier.create(successful).expectNext(Boolean.TRUE).verifyComplete();

    Assertions.assertTrue(fileOpened.get());
    Assertions.assertTrue(writtenToFile.get());
    Assertions.assertTrue(fileClosed.get());
}

/**
 * Before reading from a file we need to open file first.
 */
@Test
public void one_to_n() {
    //todo: feel free to change code as you need
    Flux<String> fileLines = null;
    Mono<Void> n1 = openFile();
    Flux<String> n2 = readFile();
    fileLines = n1.thenMany(n2);
    //        fileLines = n1.flatMapMany(ignored -> n2); // 因为n1只有complte的信号, 没有元素

    StepVerifier.create(fileLines).expectNext("0x1", "0x2", "0x3").verifyComplete();
}

/**
 * Execute all tasks sequentially and after each task have been executed, commit task changes. Don't lose id's of
 * committed tasks, they are needed to further processing!
 */
@Test
public void acid_durability() {
    //todo: feel free to change code as you need
    Flux<String> committedTasksIds = null;
    //        Flux<String> stringFlux = tasksToExecute().flatMapSequential(Mono::flux);
    //        Flux<Void> voidFlux = stringFlux.map(this::commitTask).flatMap(Mono::flux);
    //        committedTasksIds = voidFlux.thenMany(stringFlux); stringFlux会重复执行
    //        Flux<String> stringFlux = tasksToExecute().flatMapSequential(tid -> tid.flatMap(this::commitTask).then(tid));
    //        Flux<String> stringFlux = tasksToExecute().flatMapSequential(tid -> tid.flatMap(this::commitTask).then(tid).flux());
    Flux<String> stringFlux = tasksToExecute().flatMapSequential(Mono::flux);
    Flux<String> stringFlux1 = stringFlux.flatMap(t -> commitTask(t).thenReturn(t));
    committedTasksIds = stringFlux1;

    //don't change below this line
    StepVerifier.create(committedTasksIds).expectNext("task#1", "task#2", "task#3").verifyComplete();

    Assertions.assertEquals(3, committedTasksCounter.get());
}

/**
 * News have come that Microsoft is buying Blizzard and there will be a major merger.
 * Merge two companies, so they may still produce titles in individual pace but as a single company.
 */
@Test
public void major_merger() {
    //todo: feel free to change code as you need
    Flux<String> microsoftBlizzardCorp = microsoftTitles();
    Flux<String> stringFlux = blizzardTitles();
    microsoftBlizzardCorp = microsoftBlizzardCorp.mergeWith(stringFlux);

    //don't change below this line
    StepVerifier.create(microsoftBlizzardCorp)
        .expectNext("windows12", "wow2", "bing2", "overwatch3", "office366", "warcraft4")
        .verifyComplete();
}

/**
 * Your job is to produce cars. To produce car you need chassis and engine that are produced by a different
 * manufacturer. You need both parts before you can produce a car.
 * Also, engine factory is located further away and engines are more complicated to produce, therefore it will be
 * slower part producer.
 * After both parts arrive connect them to a car.
 */
@Test
public void car_factory() {
    //todo: feel free to change code as you need
    Flux<Car> producedCars = null;
    Flux<Chassis> chassisFlux = carChassisProducer();
    Flux<Engine> engineFlux = carEngineProducer();
    producedCars = chassisFlux.zipWith(engineFlux, Car::new);

    //don't change below this line
    StepVerifier.create(producedCars)
        .recordWith(ConcurrentLinkedDeque::new)
        .expectNextCount(3)
        .expectRecordedMatches(cars -> cars.stream().allMatch(car -> Objects.equals(car.chassis.getSeqNum(), car.engine.getSeqNum())))
        .verifyComplete();
}

/**
 * When `chooseSource()` method is used, based on current value of sourceRef, decide which source should be used.
 */

//only read from sourceRef
AtomicReference<String> sourceRef = new AtomicReference<>("X");

//todo: implement this method based on instructions
public Mono<String> chooseSource() {
    sourceA(); //<- choose if sourceRef == "A"
    sourceB(); //<- choose if sourceRef == "B"
    //        return  Mono.defer(() -> {
    //            if (Objects.equals(sourceRef.get(), "A")) {
    //                return sourceA();
    //            } else if (Objects.equals(sourceRef.get(), "B")) {
    //                return sourceB();
    //            } else {
    //                return Mono.empty();
    //            }
    //        });//otherwise, return empty

    return Mono.fromCallable(() -> sourceRef.get()).flatMap(s -> {
        if (Objects.equals(s, "A")) {
            return sourceA();
        } else if (Objects.equals(s, "B")) {
            return sourceB();
        } else {
            return Mono.empty();
        }
    });
}

@Test
public void deterministic() {
    //don't change below this line
    Mono<String> source = chooseSource();

    sourceRef.set("A");
    StepVerifier.create(source).expectNext("A").verifyComplete();

    sourceRef.set("B");
    StepVerifier.create(source).expectNext("B").verifyComplete();
}

/**
 * Sometimes you need to clean up after your self.
 * Open a connection to a streaming service and after all elements have been consumed,
 * close connection (invoke closeConnection()), without blocking.
 *
 * This may look easy...
 */
@Test
public void cleanup() {
    BlockHound.install(); //don't change this line, blocking = cheating!

    //todo: feel free to change code as you need
    Flux<String> stream = StreamingConnection.startStreaming().flatMapMany(Function.identity());
    Mono<Void> closeSignal = StreamingConnection.closeConnection();

    //        stream = stream.doOnEach((signal)-> {
    //            if (signal.isOnComplete()) {
    //                closeSignal.subscribe();
    //            }
    //        });
    stream = stream.doOnComplete(closeSignal::subscribe);
    //don't change below this line
    StepVerifier.create(stream).then(() -> Assertions.assertTrue(StreamingConnection.isOpen.get())).expectNextCount(20).verifyComplete();
    Assertions.assertTrue(StreamingConnection.cleanedUp.get());
}
}
