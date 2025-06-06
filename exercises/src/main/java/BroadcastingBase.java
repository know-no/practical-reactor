import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Stefan Dragisic
 */
public class BroadcastingBase {

    public Flux<String> systemUpdates() {
        return Flux.just("RESTARTED", "UNHEALTHY", "HEALTHY", "DISK_SPACE_LOW", "OOM_DETECTED", "CRASHED", "UNKNOWN")
                   .delayElements(Duration.ofSeconds(1))
                   .doOnNext(n -> System.out.println("Broadcast update: " + n));
    }

    AtomicInteger counter = new AtomicInteger(0);

    // generate 操作符会受到下游的预取（prefetch）策略影响。
    public Flux<Message> messageStream() {
        return Flux.<Integer>generate(sink -> {

                       int id = counter.getAndIncrement();
                       System.out.println("Generated: " + id);
                       sink.next(id);
                   })
//            .limitRate(1)
//            .limitRequest(10)
                   .map(i -> new Message("user#" + i, "payload#" + i))
                   .delayElements(Duration.ofMillis(250))
                   .take(5);
    }

    public static class Message {

        public String user = "";
        public String payload = "";

        public Message(String user, String payload) {
            this.user = user;
            this.payload = payload;
        }
    }
}
