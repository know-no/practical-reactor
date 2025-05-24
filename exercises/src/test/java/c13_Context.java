import org.junit.jupiter.api.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Often we might require state when working with complex streams. Reactor offers powerful context mechanism to share
 * state between operators, as we can't rely on thread-local variables, because threads are not guaranteed to be the
 * same. In this chapter we will explore usage of Context API.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#context
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c13_Context extends ContextBase {

    /**
     * You are writing a message handler that is executed by a framework (client). Framework attaches a http correlation
     * id to the Reactor context. Your task is to extract the correlation id and attach it to the message object.
     */
    public Mono<Message> messageHandler(String payload) {
        //todo: do your changes withing this method
        return Mono.deferContextual(ctx -> Mono.just(new Message(ctx.get(HTTP_CORRELATION_ID), payload)));
//        return Mono.just(new Message("set correlation_id from context here", payload));
    }

    @Test
    public void message_tracker() {
        //don't change this code
        Mono<Message> mono = messageHandler("Hello World!")
                .contextWrite(Context.of(HTTP_CORRELATION_ID, "2-j3r9afaf92j-afkaf"));

        StepVerifier.create(mono)
                    .expectNextMatches(m -> m.correlationId.equals("2-j3r9afaf92j-afkaf") && m.payload.equals(
                            "Hello World!"))
                    .verifyComplete();
    }

    /**
     * Following code counts how many times connection has been established. But there is a bug in the code. Fix it.
     */
    @Test
    public void execution_counter() {
        Mono<Void> repeat = Mono.deferContextual(ctx -> {
            ctx.get(AtomicInteger.class).incrementAndGet();
            return openConnection();
        }).contextWrite(Context.of(AtomicInteger.class, new AtomicInteger(0)));
        //todo: change this line only
        ;

        StepVerifier.create(repeat.repeat(4))
                    .thenAwait(Duration.ofSeconds(10))
                    .expectAccessibleContext()
                    .assertThat(ctx -> {
                        assert ctx.get(AtomicInteger.class).get() == 5;
                    }).then()
                    .expectComplete().verify();
    }

    /**
     * You need to retrieve 10 result pages from the database.
     * Using the context and repeat operator, keep track of which page you are on.
     * If the error occurs during a page retrieval, log the error message containing page number that has an
     * error and skip the page. Fetch first 10 pages.
     */
    @Test
    public void pagination() {
        AtomicInteger pageWithError = new AtomicInteger(); //todo: set this field when error occurs
        // 1. 生成代表页码的 Flux (0 到 10)
        Flux<Integer> results = Flux.range(0, 10)
            // 2. 对于每个页码，异步地获取页面数据
            .flatMap(pageNumber ->
                // 3. deferContextual 允许访问上下文，尽管在这个特定例子中 pageNumber 主要通过闭包传递
                Flux.deferContextual(contextView -> {
                        // 打印当前正在处理的页码，可以从闭包获取，也可以从 contextView.get("page") 获取
                        // System.out.println("Attempting to process page (from closure): " + pageNumber);
                        // if (contextView.hasKey("page")) {
                        //     System.out.println("Attempting to process page (from context): " + contextView.get("page"));
                        // }
                    System.out.println("thread " + Thread.currentThread().getName());

                        // 4. 延迟执行 getPage 操作
                        return Mono.defer(() -> {
                                System.out.println("Fetching page: " + pageNumber + " " + Thread.currentThread().getName());
                                return getPage(pageNumber); // 模拟获取页面，第3页会抛出异常
                            })
                            // 5. 将 Mono<Page> 转换为 Flux<Integer> (页面结果)
                            .flatMapMany(Page::getResult)
                            // 6. 错误处理：如果上游发生错误 (比如 getPage 或 getResult 内部的错误)
                            // onErrorContinue 会捕获错误，执行提供的 BiConsumer，然后继续处理序列中的下一个元素 (即下一页)
                            // obj 参数是导致错误的值，如果错误发生在 Mono<Page> 本身（如构造函数），obj 可能为 null
                            .onErrorResume((error) -> {
                                pageWithError.set(pageNumber); // 记录发生错误的页码
                                System.err.println("Error on page " + pageNumber + ": " + error.getMessage() + ". Skipping this page.");
                                // 此处不需要返回 Flux.empty()，onErrorContinue 会自动跳过当前错误的流元素
                                return Flux.empty();
                            }); // .subscribeOn(Schedulers.parallel()); // 如果把它订阅到具有多个线程的线程池, 就可以实现并发

                    })
                    // 7. 将当前页码写入上下文，可供 deferContextual 内的 contextView 读取
                    .contextWrite(Context.of("page", pageNumber))
            ).subscribeOn(Schedulers.boundedElastic());

        //don't change this code
        StepVerifier.create(results)
                    .expectNextCount(90)
                    .verifyComplete();

        Assertions.assertEquals(3, pageWithError.get());
    }


    /**
     * 递归辅助方法，页码的获取和递增通过 Context 中的 AtomicInteger 完成。
     *
     * @param globalPageWithError 用于记录错误页码的外部 AtomicInteger
     * @param totalPagesToAttempt 总共尝试获取的页面数
     * @return 返回一个 Flux，包含所有成功页面的结果
     */
    private Flux<Integer> fetchPagesIncrementingViaContext(AtomicInteger globalPageWithError, int totalPagesToAttempt) {
        // Step 1: 使用 deferContextual 来访问 Context
        return Flux.deferContextual(contextView -> {
            // Step 2: 从 Context 中获取共享的 AtomicInteger pageCounter
            AtomicInteger contextPageCounter = contextView.get("pageCounter");

            // Step 3: 获取当前页码并原子性地递增计数器以供下一次使用
            int currentPageNum = contextPageCounter.getAndIncrement();

            // 基本停止条件：如果当前页码已达到要尝试的总页数，则停止
            if (currentPageNum >= totalPagesToAttempt) {
                return Flux.empty();
            }

            // Step 4: 处理当前页面
            // currentPageNum 是从 Context 中的 AtomicInteger 获取的
            Flux<Integer> currentPageResults = Mono.defer(() -> {
                    System.out.println("Fetching page from context-counter: " + currentPageNum + " " + Thread.currentThread().getName());
                    // 调用 getPage 时，我们必须传递 pageNumber。
                    // 这个 pageNumber 现在完全来源于 Context 中的 AtomicInteger。
                    return getPage(currentPageNum);
                })
                .flatMapMany(Page::getResult)
                .onErrorResume(error -> {
                    globalPageWithError.set(currentPageNum); // 记录错误页码
                    System.err.println("Error on page " + currentPageNum + ": " + error.getMessage() + ". Skipping this page.");
                    return Flux.empty(); // 出错则返回空流
                });

            // Step 5: 延迟递归调用以获取下一页的结果。
            // 由于我们没有使用 contextWrite 来改变上下文，
            // 下一次 fetchPagesIncrementingViaContext 的 deferContextual 将看到相同的 Context 实例，
            // 从而访问到同一个 AtomicInteger 实例（它的内部值已经被 getAndIncrement 更新了）。
            Flux<Integer> nextPageResults = Flux.defer(() ->
                fetchPagesIncrementingViaContext(globalPageWithError, totalPagesToAttempt)
            );

            // 按顺序连接当前页和后续页的结果
            return Flux.concat(currentPageResults, nextPageResults);
        });
    }

    @Test
    public void pagination2() { // Renamed test method to original for consistency
        AtomicInteger pageWithError = new AtomicInteger(); // 用于记录哪个页面出错
        int totalPagesToFetch = 10; // 尝试获取10个页面 (页码 0 到 9)

        // 创建一个 AtomicInteger，它将被放入 Context 中并由流的各个部分共享和修改
        AtomicInteger sharedPageCounter = new AtomicInteger(0);

        Flux<Integer> results = fetchPagesIncrementingViaContext(pageWithError, totalPagesToFetch)
            // Step 0: 将共享的 pageCounter 放入初始 Context 中
            .contextWrite(Context.of("pageCounter", sharedPageCounter));

        // StepVerifier 和断言保持不变
        StepVerifier.create(results)
            .expectNextCount(90)
            .verifyComplete();

        Assertions.assertEquals(3, pageWithError.get());
    }



    /**
     * 递归辅助方法，用于获取指定页码及其后续页面的结果。
     * 页码通过参数 "递增"，并通过 Context 传递给每个页面的处理逻辑。
     *
     * @param pageWithError 用于记录错误页码的 AtomicInteger
     * @param totalPagesToAttempt 总共尝试获取的页面数 (例如 10，对应页码 0-9)
     * @return 返回一个 Flux，包含当前页面及所有后续成功页面的结果
     */
    private Flux<Integer> fetchPagesRecursively( AtomicInteger pageWithError, int totalPagesToAttempt) {
        // 基本停止条件：如果当前页码达到了要尝试的总页数，则停止递归
//        if (pageNum >= totalPagesToAttempt) {
//            return Flux.empty();
//        }

        // 使用 deferContextual 来为当前页面的处理创建一个上下文作用域
        return Flux.deferContextual(contextView -> {
            // contextView 现在包含了从 contextWrite 传入的 "currentPageNum"
            // 我们可以打印或使用它，例如：
            // System.out.println("Processing page " + contextView.get("currentPageNum") + " using context.");
            Integer pageNum = contextView.getOrDefault("currentPageNum", 0);
            if(pageNum >= totalPagesToAttempt){
                return Flux.empty();
            }

            // 获取当前页面的结果
            Flux<Integer> currentPageResults = Mono.defer(() -> {
                    // 此处的 pageNum 来自递归函数的参数，但 contextView.get("currentPageNum") 也是可用的
                    System.out.println("Fetching page: " + pageNum);
                    return getPage(pageNum); // 调用你提供的 getPage
                })
                .flatMapMany(Page::getResult) // 调用你提供的 Page 类的 getResult
                .onErrorResume(error -> {
                    pageWithError.set(pageNum); // 记录错误页码
                    System.err.println("Error on page " + pageNum + ": " + error.getMessage() + ". Skipping this page.");
                    return Flux.empty(); // 出错则返回空流，跳过此页
                }).contextWrite(Context.of("currentPageNum", pageNum + 1));

            // 延迟递归调用以获取下一页的结果
            // "pageNum + 1" 实现了页码的“自动递增”
            Flux<Integer> nextPageResults = Flux.defer(() ->
                fetchPagesRecursively(pageWithError, totalPagesToAttempt)
            ).contextWrite(Context.of("currentPageNum", pageNum + 1));

            // 将当前页面的结果与后续页面的结果连接起来
            return Flux.concat(currentPageResults, nextPageResults);
        });
    }

    @Test
    public void paginationWithContextDrivenIncrement() {
        AtomicInteger pageWithError = new AtomicInteger();
        int totalPagesToFetch = 10; // 我们要尝试获取10个页面 (0到9)

        // 调用递归函数，从第0页开始
        Flux<Integer> results = fetchPagesRecursively(pageWithError, totalPagesToFetch);//.contextWrite(Context.of("currentPageNum", 0));

        // StepVerifier 和断言保持不变
        StepVerifier.create(results)
            .expectNextCount(90) // 仍然期望90个元素 (页面0,1,2,4,5,6,7,8,9 * 10个元素/页)
            .verifyComplete();

        Assertions.assertEquals(3, pageWithError.get()); // 第3页出错
    }




    /**
     * 递归辅助方法，生成一个 "任务流" (Flux<Publisher<Integer>>)。
     * 每个 Publisher<Integer> 代表获取和处理单个页面的操作。
     * 页码的获取和为下一次递归设置均通过 Context 完成。
     * 此方法签名中没有页码参数。
     *
     * @param globalPageWithError 用于记录错误页码的外部 AtomicInteger
     * @param totalPagesToAttempt 总共尝试获取的页面数
     * @return 返回一个 Flux，其每个元素是一个用于处理单页的 Publisher<Integer>
     */
    private Flux<Publisher<Integer>> generatePageTasksConcurrently(AtomicInteger globalPageWithError, int totalPagesToAttempt) {
        return Flux.deferContextual(contextView -> {
            int currentPageNum = contextView.get("currentPageNum");

            if (currentPageNum >= totalPagesToAttempt) {
                return Flux.empty(); // 基本停止条件：没有更多任务可生成
            }

            // 创建处理当前页面的 "任务" (一个 Flux<Integer>)
            // 这个任务在被订阅时，才真正执行 getPage 等操作
            Flux<Integer> taskForCurrentPage = Mono.defer(() -> {
                    // 打印信息时可以带上线程名，观察并发性
                    System.out.println("Initiating task for page (from context): " + currentPageNum + " on thread " + Thread.currentThread().getName());
                    return getPage(currentPageNum);
                })
                .flatMapMany(Page::getResult)
                .onErrorResume(error -> {
                    globalPageWithError.set(currentPageNum);
                    System.err.println("Error on page " + currentPageNum + ": " + error.getMessage() + ". Skipping this page.");
                    return Flux.empty();
                })
                // 关键：使这个任务在被订阅时，在IO密集型操作适合的线程池上执行
                .subscribeOn(Schedulers.boundedElastic());

            // 延迟递归调用以生成后续页面的任务流
            Flux<Publisher<Integer>> nextTasksStream = Flux.defer(() ->
                generatePageTasksConcurrently(globalPageWithError, totalPagesToAttempt)
            ).contextWrite(parentContext -> parentContext.put("currentPageNum", currentPageNum + 1));

            // 发出当前页面的任务，然后连接（concatWith）后续页面的任务流。
            // 注意：这里的 concat 是针对任务本身的生成顺序，而不是任务的执行。
            // 我们按顺序生成这些待执行的任务。
            return Flux.just((Publisher<Integer>) taskForCurrentPage).concatWith(nextTasksStream);
        });
    }

    @Test
    public void paginationWithConcurrentRecursiveTaskGeneration() {
        AtomicInteger pageWithError = new AtomicInteger();
        int totalPagesToFetch = 10;
        int maxConcurrency = 4; // 设置并发获取页面的最大数量

        // 1. 使用递归辅助方法生成一个 "任务流" Flux<Publisher<Integer>>
        Flux<Publisher<Integer>> tasksStream =
            generatePageTasksConcurrently(pageWithError, totalPagesToFetch)
                .contextWrite(Context.of("currentPageNum", 0)); // 初始化Context，起始页码为0

        // 2. 使用 Flux.merge 并发执行这些任务
        // Flux.merge 会同时订阅 tasksStream 中最多 maxConcurrency 个 Publisher<Integer>
        // 由于每个 Publisher<Integer> (即 taskForCurrentPage) 内部有 .subscribeOn()，
        // 所以它们的 getPage 调用可以并发执行。
        Flux<Integer> results = Flux.merge(tasksStream, maxConcurrency);

        // 如果你想用 flatMap 实现类似效果（更常见）:
        // Flux<Integer> results = tasksStream.flatMap(taskPublisher -> taskPublisher, maxConcurrency);


        // 由于 Flux.merge 的结果是交错的，顺序无法保证，但总数应正确
        StepVerifier.create(results)
            .expectNextCount(90)
            .verifyComplete();

        Assertions.assertEquals(3, pageWithError.get());
    }


    @Test
    public void paginationWithRepeatAndContext() {
        AtomicInteger pageWithError = new AtomicInteger(); // 用于记录哪个页面出错
        final int totalPagesToAttempt = 10; // 尝试获取10个页面 (页码 0 到 9)
        final int maxConcurrency = 4;     // 并发获取页面的最大数量

        // 创建一个 AtomicInteger，它将被放入 Context 中并由流的各个部分共享和修改
        AtomicInteger contextPageCounter = new AtomicInteger(0);
//        repeat() 操作符通过对上游流（Flux.deferContextual(...) 返回的那个 Flux）进行重新订阅来工作。
//        Context 的传播: 当 repeat() 触发一次新的订阅时，这个新的订阅通常会看到在其订阅路径上建立起来的 Context。
        //如果在 repeat() 之前用 contextWrite(Context.of("pageNum", 0)) 设置了一个包含不可变 int 的 Context，
        //那么每一次重新订阅，Flux.deferContextual 都会访问到这个相同的、包含初始值 0 的 Context。它不会自动看到一个在前一次重复中“被修改”的 Context，
        // 因为 Context 本身设计上是倾向于不可变的，contextWrite 通常是为下游创建新的 Context 视图。

        // 定义获取和处理单个页面的操作。
        // 这个 Mono 在被订阅时，会产生一个 Flux<Integer> 代表该页面的结果流。
        Mono<Flux<Integer>> singlePageOperation = Mono.deferContextual(contextView -> {
            // 从 Context 中获取共享的 pageCounter
            AtomicInteger counter = contextView.get("pageCounter");
            // 获取当前页码，并为下一次重复递增计数器
            int currentPageNum = counter.getAndIncrement();

            // 理论上，repeat(totalPagesToAttempt - 1) 会精确控制执行次数。
            // 但作为防御性编程，可以保留此检查。
            if (currentPageNum >= totalPagesToAttempt) {
                System.out.println("Attempted to fetch page " + currentPageNum + " which is out of bounds. Returning empty.");
                return Mono.just(Flux.<Integer>empty());
            }

            System.out.println("Task for page " + currentPageNum + " (from context counter) initiated.");

            // 为当前页面创建一个 Flux<Integer> 任务
            Flux<Integer> pageResultsFlux = Mono.defer(() -> { // 延迟实际的 getPage 调用
                    System.out.println("Fetching page: " + currentPageNum + " on thread " + Thread.currentThread().getName());
                    return getPage(currentPageNum); // 用户的 getPage 方法
                })
                .flatMapMany(Page::getResult) // 用户的 Page 类
                .onErrorResume(error -> {
                    pageWithError.set(currentPageNum);
                    System.err.println("Error on page " + currentPageNum + ": " + error.getMessage() + ". Skipping this page.");
                    return Flux.empty(); // 发生错误则替换为空 Flux
                })
                // 关键：使每个页面的处理在独立的线程上执行，以实现并发
                .subscribeOn(Schedulers.boundedElastic());

            return Mono.just(pageResultsFlux); // 此 Mono 发出为当前页创建的 Flux<Integer> 任务
        });

        // 重复 singlePageOperation (totalPagesToAttempt - 1) 次，总共执行 totalPagesToAttempt 次。
        // 每次重复都会从 contextPageCounter 获取新的页码。
        // 这会产生一个 Flux<Flux<Integer>>，即页面结果流的流。
        Flux<Flux<Integer>> streamOfPageResultFluxes = singlePageOperation.repeat(totalPagesToAttempt - 1);

        // 并发地执行这些页面专属的 Flux，并合并它们的结果。
        Flux<Integer> results = Flux.merge(streamOfPageResultFluxes, maxConcurrency);
        // 或者使用 flatMap:
        // Flux<Integer> results = streamOfPageResultFluxes.flatMap(flux -> flux, maxConcurrency);

        // 将包含共享 pageCounter 的 Context 附加到整个操作链。
        // 当 StepVerifier 订阅时，此 Context 将可用于所有上游操作。
        Flux<Integer> finalResultsWithContext = results.contextWrite(Context.of("pageCounter", contextPageCounter));

        StepVerifier.create(finalResultsWithContext)
            .expectNextCount(90)
            .verifyComplete();

        Assertions.assertEquals(3, pageWithError.get());
    }
}
