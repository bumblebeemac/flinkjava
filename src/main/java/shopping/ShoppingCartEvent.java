package shopping;

import java.time.Instant;

public interface ShoppingCartEvent {
    String getUserId();
    Instant getTime();
}
