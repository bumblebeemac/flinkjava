package shopping;

import java.time.Instant;

public class RemovedFromShoppingCartEvent implements ShoppingCartEvent {
    private String userId;
    private String sku;
    private int quantity;
    private Instant time;

    public RemovedFromShoppingCartEvent(String userId, String sku, int quantity, Instant time) {
        this.userId = userId;
        this.sku = sku;
        this.quantity = quantity;
        this.time = time;
    }

    @Override
    public String getUserId() {
        return userId;
    }

    @Override
    public Instant getTime() {
        return time;
    }

    public String getSku() {
        return sku;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public void setTime(Instant time) {
        this.time = time;
    }
}
