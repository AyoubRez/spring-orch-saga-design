package com.appsdeveloperblog.orders.service.handler;

import com.appsdeveloperblog.core.dto.commands.ApproveOrderCommand;
import com.appsdeveloperblog.orders.service.OrderService;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics = {"${orders.commands.topic.name}"})
public class OrdersCommandHandler {

    private final OrderService orderService;

    public OrdersCommandHandler(OrderService orderService) {
        this.orderService = orderService;
    }


    @KafkaHandler
    public void handleCommand(@Payload ApproveOrderCommand command) {
        orderService.approveOrder(command.getOrderId());
    }
}
