package main.java.com.example.rxjava;

import java.util.Arrays;
import java.util.List;

public class Task1_1_ImperativeToFunctional {
    enum Status { DELIVERED, PENDING, CANCELLED }

    record Order(String id, Status status, double amount) {}

    public static void main(String[] args) {
        List<Order> orders = Arrays.asList(
                new Order("0-001", Status.DELIVERED, 1500.00),
                new Order("0-002", Status.PENDING, 300.00),
                new Order("0-003", Status.CANCELLED, 75.00),
                new Order("0-004", Status.DELIVERED, 2200.00),
                new Order("0-005", Status.PENDING, 450.00),
                new Order("0-006", Status.DELIVERED, 980.00)
        );

        double totalDelivered = orders.stream()
                .filter(order -> order.status() == Status.DELIVERED)
                .mapToDouble(Order::amount)
                .sum();

        long count = orders.stream()
                .filter(order -> order.status() == Status.DELIVERED)
                .count();

        System.out.println("Загальна сума: " + totalDelivered);
        System.out.println("Виконаних замовлень: " + count);
    }
}
