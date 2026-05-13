package main.java.com.example.rxjava;

import io.reactivex.rxjava3.core.Observable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Task3_2_FlatMapConcatMap {
    record FoodOrder(String orderId, List<String> items) {}

    public static void main(String[] args) throws InterruptedException {
        List<FoodOrder> orders = Arrays.asList(
                new FoodOrder("ZAM-01", Arrays.asList("Піца Маргарита", "Кола 0.5л")),
                new FoodOrder("ZAM-02", Arrays.asList("Борщ", "Вареники", "Компот")),
                new FoodOrder("ZAM-03", Arrays.asList("Суші-сет 20шт", "Місо-суп"))
        );

        // Частина А: flatMap без затримок (порядок зберігається)
        System.out.println("=== flatMap (без затримок) ===");
        Observable.fromIterable(orders)
                .flatMap(order -> Observable.fromIterable(order.items()))
                .map(item -> ">> " + item)
                .subscribe(System.out::println);

        // Частина В: порівняння flatMap / concatMap із затримкою
        System.out.println("\n=== flatMap (з емуляцією затримок) ===");
        Observable.fromIterable(orders)
                .flatMap(order -> Observable.fromIterable(order.items())
                        .delay(500, TimeUnit.MILLISECONDS)
                        .map(item -> "flatMap >> " + item))
                .subscribe(System.out::println);

        TimeUnit.SECONDS.sleep(3);

        System.out.println("\n=== concatMap (з емуляцією затримок) ===");
        Observable.fromIterable(orders)
                .concatMap(order -> Observable.fromIterable(order.items())
                        .delay(500, TimeUnit.MILLISECONDS)
                        .map(item -> "concatMap >> " + item))
                .subscribe(System.out::println);

        TimeUnit.SECONDS.sleep(3);
    }
}