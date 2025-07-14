package com.appsdeveloperblog.core.dto.events;

import java.math.BigDecimal;
import java.util.UUID;

public class ProductReservedEvent {

    private UUID productId;
    private UUID orderId;
    private BigDecimal productPrice;
    private Integer quantity;

    public ProductReservedEvent() {
    }

    public ProductReservedEvent(UUID productId, UUID orderId, BigDecimal productPrice, Integer quantity) {
        this.productId = productId;
        this.orderId = orderId;
        this.productPrice = productPrice;
        this.quantity = quantity;
    }

    public UUID getProductId() {
        return productId;
    }

    public void setProductId(UUID productId) {
        this.productId = productId;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public void setOrderId(UUID orderId) {
        this.orderId = orderId;
    }

    public BigDecimal getProductPrice() {
        return productPrice;
    }

    public void setProductPrice(BigDecimal productPrice) {
        this.productPrice = productPrice;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }
}
