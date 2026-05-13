package com.example.rxjava;                         // виправлено

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Task6_2_Backpressure {

    public static void main(String[] args) throws InterruptedException {
        // Частина А: buffer
        List<String> eventList = List.of(
                "LOGIN:user1", "CLICK:btn_buy", "VIEW:product_42",
                "LOGIN:user2", "LOGOUT:user1", "CLICK:btn_cart",
                "VIEW:product_7", "LOGIN:user3", "CLICK:btn_pay",
                "LOGOUT:user2", "LOGIN:user4", "VIEW:product_1"
        );

        AtomicInteger batchNum = new AtomicInteger(1);
        Observable.fromIterable(eventList)                    // <- виправлено
                .buffer(5)
                .map(batch -> "[DB] Batch INSERT #" + batchNum.getAndIncrement() + ": " + batch)
                .subscribe(System.out::println);

        // Частина В: Flowable з DROP
        System.out.println("\n=== Flowable DROP ===");
        Flowable<Integer> fastProducer = Flowable.range(1, 1000);

        AtomicInteger processed = new AtomicInteger();
        AtomicInteger dropped = new AtomicInteger();

        fastProducer
                .onBackpressureDrop(item -> dropped.incrementAndGet())   // <- виправлено
                .observeOn(Schedulers.io())
                .subscribe(new Subscriber<Integer>() {
                    private Subscription sub;

                    @Override
                    public void onSubscribe(Subscription s) {
                        this.sub = s;
                        s.request(10); // обробляємо повільно
                    }

                    @Override
                    public void onNext(Integer i) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        processed.incrementAndGet();
                    }

                    @Override
                    public void onError(Throwable t) {
                        t.printStackTrace();
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("[ЗВІТ] Оброблено: " + processed.get());
                        System.out.println("[ЗВІТ] Відкинуто: " + dropped.get());
                        System.out.println("(!) Стратегія DROP: частину елементів втрачено");
                    }
                });

        Thread.sleep(5000); // чекаємо завершення асинхронної обробки
    }
}