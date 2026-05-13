package main.java.com.example.rxjava;

import io.reactivex.rxjava3.core.Single;

public class Task4_1_Single {
    public static void main(String[] args) {
        getUserById(42)
                .subscribe(
                        user -> System.out.println("(+) Знайдено: " + user),
                        error -> System.out.println("(-) Помилка: " + error.getMessage())
                );
        getUserById(-1)
                .subscribe(
                        user -> System.out.println("(+) Знайдено: " + user),
                        error -> System.out.println("(-) Помилка: " + error.getMessage())
                );
    }

    static Single<String> getUserById(int id) {
        if (id > 0) {
            return Single.just("Користувач #" + id + ": Іван Франко");
        } else {
            return Single.error(new IllegalArgumentException("ID не може бути від'ємним або нульовим"));
        }
    }
}