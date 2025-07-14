package com.appsdeveloperblog.products.service.handler;

import com.appsdeveloperblog.core.dto.Product;
import com.appsdeveloperblog.core.dto.events.commands.ReserveProductCommand;
import com.appsdeveloperblog.products.service.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics = {"${products.commands.topic.name}"})
public class ProductCommandsHandler {

    private static final Logger log = LoggerFactory.getLogger(ProductCommandsHandler.class);

    private final ProductService productService;

    public ProductCommandsHandler(ProductService productService) {
        this.productService = productService;
    }


    @KafkaHandler
    public void handleCommand(@Payload ReserveProductCommand command) {
        try {
            Product desiredProduct = new Product(command.getProductId(), command.getProductQuantity());
            Product reservedProduct = productService.reserve(desiredProduct, command.getOrderId());
        } catch (Exception e) {
            log.error(e.getLocalizedMessage(), e);
        }
    }
}
