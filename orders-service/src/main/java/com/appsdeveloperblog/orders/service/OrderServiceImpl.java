package com.appsdeveloperblog.orders.service;

import com.appsdeveloperblog.core.dto.Order;
import com.appsdeveloperblog.core.dto.events.OrderApprovedEvent;
import com.appsdeveloperblog.core.dto.events.OrderCreatedEvent;
import com.appsdeveloperblog.core.types.OrderStatus;
import com.appsdeveloperblog.orders.dao.jpa.entity.OrderEntity;
import com.appsdeveloperblog.orders.dao.jpa.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.UUID;

@Service
public class OrderServiceImpl implements OrderService {
    private final OrderRepository orderRepository;
    // Step 5 : Inject KafkaTemplate
    private final KafkaTemplate<String, Object> kafkaTemplate;
    // Step 8 : Inject OrdersEventTopicName value
    private final String ordersEventsTopicName;

    public OrderServiceImpl(OrderRepository orderRepository,
                            KafkaTemplate<String, Object> kafkaTemplate,
                            @Value("${orders.events.topic.name}") String ordersEventsTopicName
    ) {
        this.orderRepository = orderRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.ordersEventsTopicName = ordersEventsTopicName;
    }

    @Override
    public Order placeOrder(Order order) {
        OrderEntity entity = new OrderEntity();
        entity.setCustomerId(order.getCustomerId());
        entity.setProductId(order.getProductId());
        entity.setProductQuantity(order.getProductQuantity());
        entity.setStatus(OrderStatus.CREATED);
        orderRepository.save(entity);

        // Step 3 : Instancing an OrderCreatedEvent when placeOrder
        OrderCreatedEvent placeOrderCreatedEvent = new OrderCreatedEvent(
                entity.getId(),
                entity.getCustomerId(),
                entity.getProductId(),
                entity.getProductQuantity()
        );

        // Step 6 : Send KafkaEvent to Topic
        kafkaTemplate.send(ordersEventsTopicName, placeOrderCreatedEvent);

        return new Order(
                entity.getId(),
                entity.getCustomerId(),
                entity.getProductId(),
                entity.getProductQuantity(),
                entity.getStatus());
    }

    @Override
    public void approveOrder(UUID orderId) {
        OrderEntity orderEntity = orderRepository.findById(orderId).orElse(null);
        Assert.notNull(orderEntity, "Order with id " + orderId + " does not exist");
        orderEntity.setStatus(OrderStatus.APPROVED);
        orderRepository.save(orderEntity);

        OrderApprovedEvent orderApprovedEvent = new OrderApprovedEvent(orderEntity.getId());

        kafkaTemplate.send(ordersEventsTopicName, orderApprovedEvent);
    }

    @Override
    public void rejectOrder(UUID orderId) {
        OrderEntity orderEntity = orderRepository.findById(orderId).orElse(null);
        Assert.notNull(orderEntity, "Order with id " + orderId + " does not exist");
        orderEntity.setStatus(OrderStatus.REJECTED);
        orderRepository.save(orderEntity);
    }

}
