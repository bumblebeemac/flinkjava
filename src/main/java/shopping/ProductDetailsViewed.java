package shopping;

import java.time.Instant;

public class ProductDetailsViewed implements CatalogEvent {
    private String userId;
    private Instant time;
    private String productId;

    public ProductDetailsViewed(String userId, Instant time, String productId) {
        this.userId = userId;
        this.time = time;
        this.productId = productId;
    }

    @Override
    public String getUserId() {
        return userId;
    }

    @Override
    public Instant getTime() {
        return time;
    }

    public String getProductId() {
        return productId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setTime(Instant time) {
        this.time = time;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }
}
