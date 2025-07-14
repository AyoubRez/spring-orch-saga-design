package com.appsdeveloperblog.orders.saga;

import com.appsdeveloperblog.core.dto.commands.*;
import com.appsdeveloperblog.core.dto.events.*;
import com.appsdeveloperblog.core.types.OrderStatus;
import com.appsdeveloperblog.orders.service.OrderHistoryService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

// Step 1: Create OrderSaga class and annotate it with component and kafka listener with topic name
@Component
@KafkaListener(topics = {
        "${orders.events.topic.name}",
        "${products.events.topic.name}",
        "${payments.events.topic.name}"
})
public class OrderSaga {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String productsCommandsTopicName;
    private final OrderHistoryService orderHistoryService;
    private final String paymentsCommandsTopicName;
    private final String ordersCommandsTopicName;

    public OrderSaga(KafkaTemplate<String, Object> kafkaTemplate,
                     @Value("${products.commands.topic.name}") String productsCommandsTopicName,
                     OrderHistoryService orderHistoryService,
                     @Value("${payments.commands.topic.name}") String paymentsCommandsTopicName,
                     @Value("${orders.commands.topic.name}") String ordersCommandsTopicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.productsCommandsTopicName = productsCommandsTopicName;
        this.orderHistoryService = orderHistoryService;
        this.paymentsCommandsTopicName = paymentsCommandsTopicName;
        this.ordersCommandsTopicName = ordersCommandsTopicName;
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
        orderHistoryService.add(event.getOrderId(), OrderStatus.CREATED);
    }

    @KafkaHandler
    public void handleEvent(@Payload ProductReservedEvent event) {
        ProcessPaymentCommand command = new ProcessPaymentCommand(
                event.getOrderId(),
                event.getProductId(),
                event.getProductPrice(),
                event.getQuantity()
        );

        kafkaTemplate.send(paymentsCommandsTopicName, command);

    }

    @KafkaHandler
    public void handleEvent(@Payload PaymentProcessesEvent event) {
        ApproveOrderCommand command = new ApproveOrderCommand(
                event.getOrderId()
        );
        kafkaTemplate.send(ordersCommandsTopicName, command);
    }

    @KafkaHandler
    public void handleEvent(@Payload OrderApprovedEvent event) {
        orderHistoryService.add(event.getOrderId(), OrderStatus.APPROVED);
    }

    @KafkaHandler
    public void handleEvent(@Payload PaymentFailedEvent event) {
        CancelProductReservationCommand command = new CancelProductReservationCommand(
                event.getOrderId(),
                event.getProductId(),
                event.getProductQuantity()
        );
        kafkaTemplate.send(productsCommandsTopicName, command);
    }

    @KafkaHandler
    public void handleEvent(@Payload ProductReservationCanceledEvent event) {
        RejectOrderCommand command = new RejectOrderCommand(event.getOrderId());
        kafkaTemplate.send(ordersCommandsTopicName, command);
        orderHistoryService.add(event.getOrderId(), OrderStatus.REJECTED);
    }
}
