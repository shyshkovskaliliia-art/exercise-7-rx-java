package main.java.com.example.rxjava;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Task5_2_ParallelProcessing {
    record ServiceCall(String serviceName, int delayMs) {}

    public static void main(String[] args) throws InterruptedException {
        List<ServiceCall> services = Arrays.asList(
                new ServiceCall("UserService", 800),
                new ServiceCall("OrderService", 1200),
                new ServiceCall("RecommendationService", 600)
        );

        // Частина А: послідовно (concatMap)
        long startA = System.currentTimeMillis();
        Observable.fromIterable(services)
                .concatMap(service ->
                        Observable.timer(service.delayMs, TimeUnit.MILLISECONDS)
                                .map(tick -> "Послідовно: " + service.serviceName() + " відповів за " + service.delayMs() + " мс")
                )
                .blockingSubscribe(System.out::println);
        System.out.println("Загальний час (послідовно): " + (System.currentTimeMillis() - startA) + " мс\n");

        // Частина В: паралельно (flatMap)
        long startB = System.currentTimeMillis();
        Observable.fromIterable(services)
                .flatMap(service ->
                        Observable.timer(service.delayMs, TimeUnit.MILLISECONDS)
                                .map(tick -> Thread.currentThread().getName() + " (+) " + service.serviceName() + " відповів за " + service.delayMs() + " мс")
                                .subscribeOn(Schedulers.io())
                )
                .blockingSubscribe(System.out::println);
        System.out.println("Загальний час (паралельно): ~" + (System.currentTimeMillis() - startB) + " мс");
    }
}