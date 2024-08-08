package shopping;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

public class SingleShoppingCartEventsGenerator extends EventGenerator<ShoppingCartEvent> {
  private int sleepMillisBetweenEvents;
  private Instant baseInstant;
  private Optional<Long> extraDelayInMillisOnEveryTenEvents;
  private Optional<String> sourceId;
  private boolean generateRemoved;

  public SingleShoppingCartEventsGenerator(int sleepMillisBetweenEvents, Instant baseInstant, Optional<Long> extraDelayInMillisOnEveryTenEvents, Optional<String> sourceId, boolean generateRemoved) {
    super(
      sleepMillisBetweenEvents,
      eventGenerator(
        generateRemoved,
        () -> sourceId
          .map(id -> id + " " + UUID.randomUUID())
          .orElse(UUID.randomUUID().toString()),
        baseInstant
      ),
      baseInstant,
      extraDelayInMillisOnEveryTenEvents
    );
    this.sleepMillisBetweenEvents = sleepMillisBetweenEvents;
    this.baseInstant = baseInstant;
    this.extraDelayInMillisOnEveryTenEvents = extraDelayInMillisOnEveryTenEvents;
    this.sourceId = sourceId;
    this.generateRemoved = generateRemoved;
  }

  private static List<String> users = List.of("Bob", "Alice", "Sam", "Tom", "Diana");
  private static Random random = new Random();

  public static String getRandomUser() {
    return users.get(random.nextInt(users.size()));
  }

  private static int getRandomQuantity() {
    return random.nextInt(10);
  }

  private static Function<Long, ShoppingCartEvent> eventGenerator(boolean generateRemoved, Supplier<String> skuGen, Instant baseInstant) {
    return id -> {
      if (!generateRemoved || random.nextBoolean())
        return new AddToShoppingCartEvent(
          getRandomUser(),
          skuGen.get(),
          getRandomQuantity(),
          baseInstant.plusSeconds(id)
        );
      else
        return new RemovedFromShoppingCartEvent(
          getRandomUser(),
          skuGen.get(),
          getRandomQuantity(),
          baseInstant.plusSeconds(id)
        );
    };
  }
}
