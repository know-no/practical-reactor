import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * It's time introduce some resiliency by recovering from unexpected events!
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#which.errors
 * https://projectreactor.io/docs/core/release/reference/#error.handling
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c7_ErrorHandling extends ErrorHandlingBase {


    @Test
    public void test3() {
        /**
         * 1. 标准的“信号传播”模型 (doOnError, onErrorResume)
         * 在通常情况下，当一个操作符（如 map）内部发生错误时，它会做一件事：向下游传播一个 onError 终止信号。
         * 这个信号会沿着操作链一直往下走，直到被某个操作符处理（如 onErrorResume）或者最终到达 subscribe 中的错误处理部分。
         * doOnError 就是这个模型中的一个“监听者”。它位于信号传播的路径上，当 onError 信号经过它时，它会执行指定的操作（比如打印日志），然后让信号继续往下传。
         * 如果您的代码是这样写的：
         * Java
         * .map(...) // 这里抛出异常
         * .doOnError(...) // 错误信号到达，执行！
         * .onErrorResume(...) // 错误信号被消费，流切换到备用方案
         * 那么 doOnError 是会执行的。
         * 2. 特殊的“上下文恢复”模型 (onErrorContinue)
         * onErrorContinue 打破了上面的常规模型。它使用了 Reactor 的一个高级特性：上下文 (Context)。
         * 在流订阅的阶段（也就是从下往上构建执行链的时候），onErrorContinue 会做一件特殊的事情：它会把它自己的恢复逻辑（您在 onErrorContinue 中写的 lambda 表达式）注册到 Context 中。
         * 然后，当数据从上往下流动时：
         * map 操作符（以及其他很多核心操作符）是“有 Context 意识的”。在它内部的 try-catch 块中，当它捕获到一个异常时，它不会立即发送 onError 信号。
         * 相反，它会先检查 Context，看看有没有注册过“恢复函数”。
         * 它发现了 onErrorContinue 注册的函数！
         * 于是，map 直接调用了这个恢复函数，把异常和导致异常的元素 (3) 作为参数传进去。
         * onErrorContinue 的逻辑（您写的 System.err.println）被执行。
         * 执行完恢复逻辑后，map 就认为这个错误已经被“就地处理”了。它不会再向下游发送任何 onError 信号。它只会向上游请求下一个元素（request(1)），让数据流继续。
         * 因为 map 从未发出过 onError 信号，所以 doOnError 根本就没有机会监听到任何东西。
         * 一个比喻
         * onError 信号：像是一个火灾警报。一旦拉响（map 出错），警报声（onError 信号）会传遍整个楼层（下游操作链），每个房间（doOnError）都能听到，直到消防队（onErrorResume）赶来灭火。
         * onErrorContinue：像是在 map 这个房间里安装了一个自动灭火喷头（注册到 Context 的恢复函数）。一旦有小火苗（异常），喷头立刻启动把火灭了。因为火被当场扑灭，所以根本就没有机会去拉响整栋楼的火灾警报。楼下的保安（doOnError）自然什么也不知道。
         * 结论
         * doOnError 和 onErrorContinue 并不是简单的“谁先谁后”的优先级关系。它们工作在完全不同的机制上。onErrorContinue 通过 Context 从根本上改变了上游操作符处理错误的方式，阻止了 onError 信号的产生，因此无论 doOnError 放在链中的哪个位置（只要在出错点下游），它都无法被触发。
         */
        {
            System.out.println("--- onErrorContinue 正确用法 ---");
            Flux.range(1, 4) // 源头是 1, 2, 3, 4
                .doOnCancel(() -> System.out.println("上游收到取消信号")) // 不会调用
                .map(i -> {
                    if (i == 3) {
                        // 在处理元素 '3' 时抛出异常
                        throw new ArithmeticException("处理 '3' 时出错!");
                    }
                    return "成功处理: " + i;
                })

                .doOnComplete(() -> System.out.println("下游完成"))
                .doOnCancel(() -> System.out.println("下游cancel"))
                .doOnError(error -> System.out.println("下游error: " + error.getMessage()))
                 .onErrorContinue((throwable, item) -> {
//                      这个会执行！
                     System.err.println("!!! onErrorContinue 捕获到错误: " + throwable.getMessage());
                     System.err.println("!!! 跳过的元素是: " + item); // 这里的 item 就是导致错误的 '3'
                 })
                .subscribe(
                    System.out::println,
                    error -> System.err.println("最终订阅者收到错误: " + error), // 这不会被调用
                    () -> System.out.println("最终订阅者完成") // 这会被调用
                );
        }
    }

    @Test
    public void test1() {
        // 模拟一个在发出 '2' 之后会因内部错误而终止的 Flux
        Flux<String> stringFlux = Flux.create(sink -> {
            System.out.println("源头流: 发送 1");
            sink.next("1");
            System.out.println("源头流: 发送 2");
            sink.next("2");
            System.out.println("源头流: 内部发生致命错误，即将终止...");
            sink.error(new IllegalStateException("Source Flux failed!")); // sink的error信号, 下游调用onErrorContinue也不能恢复了
//            错误是通过 sink.error() 直接发出的终止信号，而不是在处理元素过程中发生的错误; 所以恢复不了
//            错误的本质：你的错误是流级别的终止信号，而不是元素级别的处理错误
            // 下面的代码不会被执行
             sink.next("3");
             sink.complete();
        });

        System.out.println("--- 订阅开始 ---");

        stringFlux
            .doOnNext(item -> System.out.println("下游收到: " + item))
            .doOnComplete(() -> System.out.println("下游处理完成x"))
            .doOnCancel(() -> System.out.println("下游处理cancel"))
            .onErrorContinue((throwable, o) -> {
                System.out.println("onErrorContinue 捕获到错误: " + throwable.getMessage());
                System.out.println("导致错误的对象: " + o); // 注意：这里 o 会是 null，因为错误是终止信号，不是由特定元素引起的
            })
            .doOnComplete(() -> System.out.println("下游处理完成y"))
            .blockLast(); // 仅为演示目的，阻塞等待

        System.out.println("--- 程序结束 ---");
    }

    @Test
    public void test() {
        Flux.just(1, 2, 3, 4, 5)
            .doOnCancel(() -> System.out.println("上游收到取消信号"))
            .map(i -> {
                if (i == 3) throw new RuntimeException("错误!");
                return i;
            })
            .subscribe(
                System.out::println,
                error -> System.out.println("错误: " + error.getMessage())
            );

        // 输出：
        // 1
        // 2
        // 上游收到取消信号
        // 错误: 错误!
    }

    /**
     * You are monitoring hearth beat signal from space probe. Heart beat is sent every 1 second.
     * Raise error if probe does not any emit heart beat signal longer then 3 seconds.
     * If error happens, save it in `errorRef`.
     */
    @Test
    public void houston_we_have_a_problem() {
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        Flux<String> heartBeat = probeHeartBeatSignal()
            .timeout(Duration.ofSeconds(3), Flux.error(new TimeoutException()))
            .doOnError(errorRef::set)
                //todo: do your changes here
                //todo: & here
                ;

        StepVerifier.create(heartBeat)
                    .expectNextCount(3)
                    .expectError(TimeoutException.class)
                    .verify();

        Assertions.assertTrue(errorRef.get() instanceof TimeoutException);
    }

    /**
     * Retrieve currently logged user.
     * If any error occurs, exception should be further propagated as `SecurityException`.
     * Keep original cause.
     */
    @Test
    public void potato_potato() {
        Mono<String> currentUser = getCurrentUser()
//            .onErrorResume(e -> Mono.error(new SecurityException(e)))
            .onErrorMap(e -> new SecurityException(e))
                //todo: change this line only
                //use SecurityException
                ;

        StepVerifier.create(currentUser)
                    .expectErrorMatches(e -> e instanceof SecurityException &&
                            e.getCause().getMessage().equals("No active session, user not found!"))
                    .verify();
    }

    /**
     * Consume all the messages `messageNode()`.
     * Ignore any failures, and if error happens finish consuming silently without propagating any error.
     */
    @Test
    public void under_the_rug() {
        Flux<String> messages = messageNode()
            .onErrorResume(e -> Flux.empty())
        //todo: change this line only
        ;

        StepVerifier.create(messages)
                    .expectNext("0x1", "0x2")
                    .verifyComplete();
    }

    /**
     * Retrieve all the messages `messageNode()`,and if node suddenly fails
     * use `backupMessageNode()` to consume the rest of the messages.
     */
    @Test
    public void have_a_backup() {
        //todo: feel free to change code as you need
        Flux<String> messages = null;
        Flux<String> m = messageNode();
        Flux<String> backup = backupMessageNode();
        messages = m.onErrorResume(e -> backup);

        //don't change below this line
        StepVerifier.create(messages)
                    .expectNext("0x1", "0x2", "0x3", "0x4")
                    .verifyComplete();
    }

    /**
     * Consume all the messages `messageNode()`, if node suddenly fails report error to `errorReportService` then
     * propagate error downstream.
     */
    @Test
    public void error_reporter() {
        //todo: feel free to change code as you need
        Flux<String> messages = messageNode()
//            .onErrorResume(e -> {
//                return errorReportService(e)
//                    .then(Mono.error(e))
//                    .map((v) -> "")
//                    .flux();
//            });
            .onErrorResume(e -> {
                return errorReportService(e)
                    .then(Mono.<String>error(e))
                    .flux();
            });

        //don't change below this line
        StepVerifier.create(messages)
                    .expectNext("0x1", "0x2")
                    .expectError(RuntimeException.class)
                    .verify();
        Assertions.assertTrue(errorReported.get());
    }

    /**
     * Execute all tasks from `taskQueue()`. If task executes
     * without any error, commit task changes, otherwise rollback task changes.
     * Do don't propagate any error downstream.
     */
    @Test
    public void unit_of_work() {
        System.out.println("start: " );
        Flux<Task> taskFlux = taskQueue()
                //todo: do your changes here
            .flatMap(task -> task.execute().then(task.commit()).onErrorResume(task::rollback).thenReturn(task))
//            .flatMap(task -> task.execute().flatMap((t) -> {
//                System.out.println("execute: " + t);
//                return task.commit();
//            }).onErrorResume(task::rollback).thenReturn(task))
                ;

        StepVerifier.create(taskFlux)
                    .expectNextMatches(task -> task.executedExceptionally.get() && !task.executedSuccessfully.get())
                    .expectNextMatches(task -> task.executedSuccessfully.get() && task.executedSuccessfully.get())
                    .verifyComplete();
    }

    /**
     * `getFilesContent()` should return files content from three different files. But one of the files may be
     * corrupted and will throw an exception if opened.
     * Using `onErrorContinue()` skip corrupted file and get the content of the other files.
     */
    @Test
    public void billion_dollar_mistake() {
        Flux<String> content = getFilesContent()
                .flatMap(Function.identity())
                .onErrorContinue((e, o) -> System.out.println("error: " + e))
                //todo: change this line only
                ;

        StepVerifier.create(content)
                    .expectNext("file1.txt content", "file3.txt content")
                    .verifyComplete();
    }

    /**
     * Quote from one of creators of Reactor: onErrorContinue is my billion-dollar mistake. `onErrorContinue` is
     * considered as a bad practice, its unsafe and should be avoided.
     *
     * {@see <a href="https://nurkiewicz.com/2021/08/onerrorcontinue-reactor.html">onErrorContinue</a>} {@see <a
     * href="https://devdojo.com/ketonemaniac/reactor-onerrorcontinue-vs-onerrorresume">onErrorContinue vs
     * onErrorResume</a>} {@see <a href="https://bsideup.github.io/posts/daily_reactive/where_is_my_exception/">Where is
     * my exception?</a>}
     *
     * Your task is to implement `onErrorContinue()` behaviour using `onErrorResume()` operator,
     * by using knowledge gained from previous lessons.
     */
    @Test
    public void resilience() {
        //todo: change code as you need
        Flux<String> content = getFilesContent()
                .flatMap(mono ->mono.onErrorResume(e -> Mono.empty()))
            ; //start from here


        //don't change below this line
        StepVerifier.create(content)
                    .expectNext("file1.txt content", "file3.txt content")
                    .verifyComplete();
    }

    /**
     * You are trying to read temperature from your recently installed DIY IoT temperature sensor. Unfortunately, sensor
     * is cheaply made and may not return value on each read. Keep retrying until you get a valid value.
     */
    @Test
    public void its_hot_in_here() {
        Mono<Integer> temperature = temperatureSensor()
            .retry()
                //todo: change this line only
                ;

        StepVerifier.create(temperature)
                    .expectNext(34)
                    .verifyComplete();
    }

    /**
     * In following example you are trying to establish connection to database, which is very expensive operation.
     * You may retry to establish connection maximum of three times, so do it wisely!
     * FIY: database is temporarily down, and it will be up in few seconds (5).
     */
    @Test
    public void back_off() {
        Mono<String> connection_result = establishConnection()
//            .retryWhen(Retry.backoff(3, Duration.ofSeconds(2)))
            .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(3)))
                //todo: change this line only
                ;

        StepVerifier.create(connection_result)
                    .expectNext("connection_established")
                    .verifyComplete();
    }

    /**
     * You are working with legacy system in which you need to read alerts by pooling SQL table. Implement polling
     * mechanism by invoking `nodeAlerts()` repeatedly until you get all (2) alerts. If you get empty result, delay next
     * polling invocation by 1 second.
     */
    @Test
    public void good_old_polling() {
        //todo: change code as you need
        Flux<String> alerts = null;
        alerts = Flux.defer(this::nodeAlerts).repeatWhen(flux -> flux.delayElements(Duration.ofSeconds(1)));
//        nodeAlerts();
        //don't change below this line
        StepVerifier.create(alerts.take(2))
                    .expectNext("node1:low_disk_space", "node1:down")
                    .verifyComplete();
    }

    public static class SecurityException extends Exception {

        public SecurityException(Throwable cause) {
            super(cause);
        }
    }
}
