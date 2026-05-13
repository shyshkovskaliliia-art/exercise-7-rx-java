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

        // Частина А: Холодний Observable (без затримок)
        System.out.println("=== Частина А: Холодний Observable ===");
        Observable<String> cold = Observable.fromIterable(results)
                .doOnNext(match -> System.out.println("[EMIT] " + match));

        System.out.println("--- Підписник 1 ---");
        cold.subscribe(m -> System.out.println("  [Sub1] " + m));

        System.out.println("--- Підписник 2 ---");
        cold.subscribe(m -> System.out.println("  [Sub2] " + m));

        // Частина В: Гарячий Observable (з емуляцією часу через interval)
        System.out.println("\n=== Частина В: Гарячий Observable ===");

        ConnectableObservable<String> hot = Observable
                .interval(500, TimeUnit.MILLISECONDS)
                .map(i -> results.get(i.intValue()))
                .take(results.size())
                .doOnNext(match -> System.out.println("[EMIT] " + match))
                .publish();

        hot.subscribe(m -> System.out.println("  [Sub1] " + m));

        System.out.println("[START] Початок випромінювання...");
        hot.connect();

        TimeUnit.SECONDS.sleep(2);
        System.out.println("--- Підключення Subscriber 2 через 2 сек ---");
        hot.subscribe(m -> System.out.println("  [Sub2] " + m));

        TimeUnit.SECONDS.sleep(2);
        System.out.println("[END] Завершення");
    }
}