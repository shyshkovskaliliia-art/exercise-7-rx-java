package main.java.com.example.rxjava;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.functions.Action;
import io.reactivex.rxjava3.functions.Consumer;

public class Task4_2_MaybeCompletable {
    public static void main(String[] args) {
        // Частина А — Maybe
        System.out.println("=== Maybe (кеш) ===");
        findInCache("user:1")
                .subscribe(
                        (Consumer<String>) value -> System.out.println("[КЕШ (+)] Знайдено: " + value),
                        (Consumer<Throwable>) error -> System.out.println("[КЕШ (!)] Помилка: " + error.getMessage()),
                        (Action) () -> System.out.println("[КЕШ (-)] Кеш-міс. Значення: Завантажено з БД")
                );

        findInCache("user:2")
                .subscribe(
                        (Consumer<String>) value -> System.out.println("[КЕШ (+)] Знайдено: " + value),
                        (Consumer<Throwable>) error -> System.out.println("[КЕШ (!)] Помилка: " + error.getMessage()),
                        (Action) () -> System.out.println("[КЕШ (-)] Кеш-міс. Значення: Завантажено з БД")
                );

        findInCache("user:error")
                .subscribe(
                        (Consumer<String>) value -> System.out.println("[КЕШ (+)] Знайдено: " + value),
                        (Consumer<Throwable>) error -> System.out.println("[КЕШ (!)] Помилка: " + error.getMessage())
                );

        // Частина В — Completable
        System.out.println("\n=== Completable (успіх) ===");
        chainRegistration(true).subscribe(
                () -> System.out.println("(+) Реєстрацію завершено успішно!"),
                error -> System.out.println("(-) Помилка реєстрації: " + error.getMessage())
        );

        System.out.println("\n=== Completable (помилка в БД) ===");
        chainRegistration(false).subscribe(
                () -> System.out.println("(+) Реєстрацію завершено успішно!"),
                error -> System.out.println("(-) Помилка реєстрації: " + error.getMessage())
        );
    }

    static Maybe<String> findInCache(String key) {
        if ("user:1".equals(key)) {
            return Maybe.just("{'name':'Леся','age':28}");
        } else if ("user:2".equals(key)) {
            return Maybe.empty();
        } else {
            return Maybe.error(new RuntimeException("Redis недоступний"));
        }
    }

    static Completable chainRegistration(boolean successDB) {
        return validateInput()
                .andThen(saveToDatabase(successDB))
                .andThen(generateToken()
                        .doOnSuccess(token -> System.out.println("[ТОКЕН] Токен: " + token))
                        .ignoreElement()
                );
    }

    static Completable validateInput() {
        return Completable.fromAction(() -> {
            System.out.println("[ПОШУК] Перевірка даних...");
            System.out.println("(+) Дані валідні");
        });
    }

    static Completable saveToDatabase(boolean success) {
        return Completable.fromAction(() -> {
            System.out.println("[DB] Збереження в БД...");
            if (!success) {
                throw new RuntimeException("Помилка збереження в БД");
            }
            System.out.println("(+) Збережено");
        });
    }

    static Single<String> generateToken() {
        return Single.just("eyJhbGci0iJIUzI1NiIsInR5cCI6IkpXVCJ9.demo");
    }
}