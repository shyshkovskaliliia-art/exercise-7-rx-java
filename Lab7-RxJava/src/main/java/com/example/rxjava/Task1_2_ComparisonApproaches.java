package main.java.com.example.rxjava;

import io.reactivex.rxjava3.core.Observable;
import java.util.Arrays;
import java.util.List;

public class Task1_2_ComparisonApproaches {
    public static void main(String[] args) {
        List<String> cities = Arrays.asList(
                "Київ", "Харків", "Одеса", "Дніпро", "Запоріжжя",
                "Кривий Ріг", "Миколаїв", "Херсон", "Кропивницький",
                "Черкаси", "Суми", "Хмельницький", "Чернівці", "Каховка"
        );

        // 1. Імперативний
        System.out.println("=== Імперативний ===");
        List<String> resultImperative = new java.util.ArrayList<>();
        for (String city : cities) {
            if (city.startsWith("К")) {
                resultImperative.add(city.toUpperCase());
            }
        }
        resultImperative.sort(String::compareTo);
        resultImperative.forEach(System.out::println);

        // 2. Функціональний
        System.out.println("=== Функціональний (Streams) ===");
        cities.stream()
                .filter(c -> c.startsWith("К"))
                .map(String::toUpperCase)
                .sorted()
                .forEach(System.out::println);

        // 3. Реактивний (RxJava)
        System.out.println("=== Реактивний (RxJava) ===");
        Observable.fromIterable(cities)
                .filter(c -> c.startsWith("К"))
                .map(String::toUpperCase)
                .sorted()
                .subscribe(System.out::println);
    }
}