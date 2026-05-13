package main.java.com.example.rxjava;

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
        System.out.println("=== Частина А: Batch INSERT ===");
        List<String> eventList = List.of(
                "LOGIN:user1", "CLICK:btn_buy", "VIEW:product_42",
                "LOGIN:user2", "LOGOUT:user1", "CLICK:btn_cart",
                "VIEW:product_7", "LOGIN:user3", "CLICK:btn_pay",
                "LOGOUT:user2", "LOGIN:user4", "VIEW:product_1"
        );

        AtomicInteger batchNum = new AtomicInteger(1);
        AtomicInteger totalEvents = new AtomicInteger(0);

        Observable.fromIterable(eventList)
                .doOnNext(event -> totalEvents.incrementAndGet())  // рахуємо всі події
                .buffer(5)
                .map(batch -> "[DB] Batch INSERT #" + batchNum.getAndIncrement() + ": " + batch)
                .subscribe(System.out::println,
                        Throwable::printStackTrace,
                        () -> System.out.println("(+) Збережено подій: " + totalEvents.get())
                );

        // Частина В: Flowable з DROP
        System.out.println("\n=== Частина В: Flowable DROP ===");
        Flowable<Integer> fastProducer = Flowable.range(1, 1000);

        AtomicInteger processed = new AtomicInteger();
        AtomicInteger dropped = new AtomicInteger();

        fastProducer
                .onBackpressureDrop(item -> dropped.incrementAndGet())
                .observeOn(Schedulers.io())
                .subscribe(new Subscriber<Integer>() {
                    private Subscription sub;

                    @Override
                    public void onSubscribe(Subscription s) {
                        this.sub = s;
                        s.request(1); // запитуємо по 1 елементу для повільної обробки
                    }

                    @Override
                    public void onNext(Integer i) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(10); // повільна обробка
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        processed.incrementAndGet();
                        sub.request(1); // запитуємо наступний елемент
                    }

                    @Override
                    public void onError(Throwable t) {
                        t.printStackTrace();
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("[ЗВІТ] Оброблено: ~" + processed.get());
                        System.out.println("[ЗВІТ] Відкинуто: ~" + dropped.get());
                        System.out.println("(!) Стратегія DROP: частину елементів втрачено");
                    }
                });

        Thread.sleep(30_000);
    }
}