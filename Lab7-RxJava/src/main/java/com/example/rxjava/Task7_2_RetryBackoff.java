package main.java.com.example.rxjava;

import io.reactivex.rxjava3.core.Observable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Task7_2_RetryBackoff {
    public static void main(String[] args) throws InterruptedException {
        AtomicInteger attemptCount = new AtomicInteger(0);

        Observable<String> unstableApiCall = Observable.create(emitter -> {
            int attempt = attemptCount.incrementAndGet();
            System.out.println("[ПОВТОР] Спроба #" + attempt);
            if (attempt < 4) {
                emitter.onError(new IOException("Connection timeout"));
            } else {
                emitter.onNext("(+) Відповідь API: {status: 'ok', data: [...]}");
                emitter.onComplete();
            }
        });

        unstableApiCall
                .retryWhen(errors ->
                        errors.zipWith(Observable.range(1, 4), (error, retryCount) -> {
                            if (retryCount < 4) {
                                long delay = (long) Math.pow(2, retryCount - 1) * 1000;
                                System.out.println("Очікуємо " + (delay / 1000) + " сек перед повтором...");
                                return Observable.timer(delay, TimeUnit.MILLISECONDS);
                            } else {
                                return Observable.error(error);
                            }
                        }).flatMap(x -> x)
                )
                .subscribe(
                        result -> System.out.println(result),
                        error -> System.out.println("Помилка після всіх спроб: " + error.getMessage())
                );

        Thread.sleep(10_000);
    }
}