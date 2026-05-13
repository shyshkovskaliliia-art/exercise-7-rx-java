package main.java.com.example.rxjava;                          // ← виправлений пакет

import io.reactivex.rxjava3.core.Observable;

public class Task7_1_ErrorHandling {
    public static void main(String[] args) {
        Observable<String> currencyService = Observable.create(emitter -> {
            emitter.onNext("USD -> UAH: 41.50");
            emitter.onNext("EUR -> UAH: 44.20");
            emitter.onError(new RuntimeException("Сервіс тимчасово недоступний"));
            emitter.onNext("GBP -> UAH: 52.10");
        });

        // Сценарій А: onErrorReturn
        System.out.println("=== onErrorReturn ===");
        currencyService
                .onErrorReturn(error -> "Використовується кешований курс: USD -> UAH: 41.00")
                .subscribe(
                        System.out::println,
                        error -> System.out.println("Помилка: " + error.getMessage())
                );

        System.out.println("\n=== onErrorResumeNext ===");
        Observable<String> currencyService2 = Observable.create(emitter -> {
            emitter.onNext("USD -> UAH: 41.50");
            emitter.onNext("EUR -> UAH: 44.20");
            emitter.onError(new RuntimeException("Сервіс тимчасово недоступний"));
        });

        currencyService2
                .onErrorResumeNext(error -> Observable.just("JPY -> UAH: 0.27", "PLN -> UAH: 10.30"))
                .subscribe(
                        System.out::println,
                        error -> System.out.println("Помилка: " + error.getMessage())
                );
    }
}