package com.appsdeveloperblog.core.dto.events.commands;

import java.util.UUID;

// Step 2 : Create ReserveProductCommand
public class ReserveProductCommand {

    private UUID productId;
    private Integer productQuantity;
    private UUID orderId;

    public ReserveProductCommand() {
    }

    public ReserveProductCommand(UUID productId, Integer productQuantity, UUID orderId) {
        this.productId = productId;
        this.productQuantity = productQuantity;
        this.orderId = orderId;
    }

    public UUID getProductId() {
        return productId;
    }

    public void setProductId(UUID productId) {
        this.productId = productId;
    }

    public Integer getProductQuantity() {
        return productQuantity;
    }

    public void setProductQuantity(Integer productQuantity) {
        this.productQuantity = productQuantity;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public void setOrderId(UUID orderId) {
        this.orderId = orderId;
    }
}
