package main.java.com.example.rxjava;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.concurrent.TimeUnit;

public class Task5_1_Schedulers {
    public static void main(String[] args) throws InterruptedException {
        Observable<String> images = Observable.just(
                "photo_1.jpg", "photo_2.jpg", "photo_3.jpg"
        );

        images
                .subscribeOn(Schedulers.io())
                .doOnNext(img -> System.out.println(
                        Thread.currentThread().getName() + " [ЗАВАНТ] Завантаження: " + img))
                .delay(1, TimeUnit.SECONDS) // імітація завантаження
                .observeOn(Schedulers.computation())
                .doOnNext(img -> System.out.println(
                        Thread.currentThread().getName() + " [СТИСК] Стиснення: " + img))
                .delay(500, TimeUnit.MILLISECONDS) // імітація стиснення
                .observeOn(Schedulers.trampoline()) // імітація головного потоку
                .doOnNext(img -> System.out.println(
                        Thread.currentThread().getName() + " [ФОТО] Відображення: " + img))
                .subscribe();

        Thread.sleep(5000);
    }
}