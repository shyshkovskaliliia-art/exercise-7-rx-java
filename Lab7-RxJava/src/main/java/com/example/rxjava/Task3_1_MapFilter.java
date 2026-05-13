package main.java.com.example.rxjava;

import io.reactivex.rxjava3.core.Observable;
import java.util.Arrays;
import java.util.List;

public class Task3_1_MapFilter {
    record Product(String name, double priceUsd) {}

    public static void main(String[] args) {
        List<Product> products = Arrays.asList(
                new Product("Навушники Sony", 49.99),
                new Product("Клавіатура Logitech", 129.00),
                new Product("Монітор LG 27\"", 399.00),
                new Product("USB-хаб Anker", 35.00),
                new Product("Веб-камера Logitech", 149.00),
                new Product("Килимок для миші", 18.00),
                new Product("SSD Samsung 1TB", 110.00)
        );

        double rate = 41.5;

        Observable.fromIterable(products)
                .filter(p -> p.priceUsd() > 100)
                .map(p -> String.format("%s -- %.2f грн (є в наявності)",
                        p.name(), p.priceUsd() * rate))
                .subscribe(System.out::println);
    }
}