package shopping;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public class CatalogEventsGenerator extends EventGenerator<CatalogEvent> {
    private int sleepMillisBetweenEvents;
    private Instant baseInstant;
    private Optional<Long> extraDelayInMillisOnEveryTenEvents;

    public CatalogEventsGenerator(int sleepMillisBetweenEvents, Instant baseInstant, Optional<Long> extraDelayInMillisOnEveryTenEvents) {
        super(
          sleepMillisBetweenEvents,
          id -> new ProductDetailsViewed(
            SingleShoppingCartEventsGenerator.getRandomUser(),
            baseInstant.plusSeconds(id), UUID.randomUUID().toString()),
            baseInstant,
          extraDelayInMillisOnEveryTenEvents
        );

        this.sleepMillisBetweenEvents = sleepMillisBetweenEvents;
        this.baseInstant = baseInstant;
        this.extraDelayInMillisOnEveryTenEvents = extraDelayInMillisOnEveryTenEvents;
    }
}