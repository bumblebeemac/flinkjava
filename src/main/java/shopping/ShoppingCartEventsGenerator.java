package shopping;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Instant;
import java.util.*;

public class ShoppingCartEventsGenerator implements SourceFunction<ShoppingCartEvent> {
    private int sleepMillisPerEvent;
    private int batchSize;
    private Instant baseInstant;
    private volatile boolean running = true;

    public ShoppingCartEventsGenerator(int sleepMillisPerEvent, int batchSize, Instant baseInstant) {
        this.sleepMillisPerEvent = sleepMillisPerEvent;
        this.batchSize = batchSize;
        this.baseInstant = baseInstant;
    }

    public ShoppingCartEventsGenerator(int sleepMillisPerEvent, int batchSize) {
        this(sleepMillisPerEvent, batchSize, Instant.now());
    }

    @Override
    public void run(SourceContext<ShoppingCartEvent> ctx) throws Exception {
        run(0, ctx);
    }

    @Override
    public void cancel() {
        running = false;
    }

    private void run(long startId, SourceContext<ShoppingCartEvent> ctx) throws Exception {
        while (running) {
            generateRandomEvents(startId).forEach(ctx::collect);
            Thread.sleep(batchSize * sleepMillisPerEvent);
            startId += batchSize;
        }
    }

    private List<AddToShoppingCartEvent> generateRandomEvents(long id) {
        List<AddToShoppingCartEvent> events = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            events.add(new AddToShoppingCartEvent(
                getRandomUser(),
                UUID.randomUUID().toString(),
                getRandomQuantity(),
                baseInstant.plusSeconds(id)
            ));
            id++;
        }
        return events;
    }

    private String getRandomUser() {
        List<String> users = List.of("Bob", "Alice", "Sam", "Tom", "Diana");
        return users.get(new Random().nextInt(users.size()));
    }

    private int getRandomQuantity() {
        return new Random().nextInt(10);
    }
}
