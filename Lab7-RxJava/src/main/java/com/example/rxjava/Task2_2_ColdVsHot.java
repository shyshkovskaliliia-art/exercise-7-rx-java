package main.java.com.example.rxjava;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.observables.ConnectableObservable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Task2_2_ColdVsHot {
    public static void main(String[] args) throws InterruptedException {
        List<String> results = Arrays.asList(
                "Динамо 2:1 Шахтар",
                "Шахтар 3:0 Металіст",
                "Зоря 1:1 Ворскла",
                "Дніпро-1 2:0 Львів",
                "Колос 0:0 Олександрія"
        );

        Observable<String> cold = Observable.fromIterable(results)
                .doOnNext(match -> System.out.println("[emit] " + match));

        // Частина А: холодний
        System.out.println("=== Холодний Observable ===");
        cold.subscribe(m -> System.out.println("Subscriber 1: " + m));
        cold.subscribe(m -> System.out.println("Subscriber 2: " + m));

        // Частина В: гарячий
        System.out.println("\n=== Гарячий Observable ===");
        ConnectableObservable<String> hot = cold.publish();

        hot.subscribe(m -> System.out.println("Subscriber 1: " + m));
        hot.connect();

        TimeUnit.SECONDS.sleep(2);
        System.out.println("--- Підключення другого підписника через 2 сек ---");
        hot.subscribe(m -> System.out.println("Subscriber 2: " + m));

        TimeUnit.SECONDS.sleep(1);
    }
}