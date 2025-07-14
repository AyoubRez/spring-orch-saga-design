package com.appsdeveloperblog.orders.saga;

import com.appsdeveloperblog.core.dto.events.OrderCreatedEvent;
import com.appsdeveloperblog.core.dto.events.commands.ReserveProductCommand;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

// Step 1: Create OrderSaga class and annotate it with component and kafka listener with topic name
@Component
@KafkaListener(topics = {"${orders.events.topic.name}"})
public class OrderSaga {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String productsCommandsTopicName;

    public OrderSaga(KafkaTemplate<String, Object> kafkaTemplate,
                     @Value("${products.commands.topic.name}") String productsCommandsTopicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.productsCommandsTopicName = productsCommandsTopicName;
    }

    // kafka handler to handler order created event
    @KafkaHandler
    public void handleEvent(@Payload OrderCreatedEvent event) {

        ReserveProductCommand command = new ReserveProductCommand(
                event.getProductId(),
                event.getProductQuantity(),
                event.getOrderId()
        );

        kafkaTemplate.send(productsCommandsTopicName, command);
    }
}
