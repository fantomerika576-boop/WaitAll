// П7. Дочекатися всіх, і що буде з рештою.

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class WaitAll {

    enum Kind {
        FAILS, SLEEPS, BURNS
    }

    /**
     * Одна задача. Сама записує, скільки прожила, чи побачила прохання зупинитися
     * і чи виконався блок очищення.
     */
    static final class Job implements Callable<String> {
        final String name;
        private final Kind kind;
        private final int ms;
        volatile long livedMs = -1;
        volatile boolean sawCancel, cleaned, finished;

        Job(String name, Kind kind, int ms) {
            this.name = name;
            this.kind = kind;
            this.ms = ms;
        }

        @Override
        public String call() throws Exception {
            long t0 = System.currentTimeMillis();
            try {
                switch (kind) {
                    case FAILS -> {
                        Thread.sleep(ms);
                        throw new IllegalStateException(name + ": джерело впало");
                    }
                    case SLEEPS -> {
                        for (int left = ms; left > 0; left -= 200)
                            Thread.sleep(200);
                    }
                    case BURNS -> burn(ms);
                }
                finished = true;
                return name + ": відпрацювала повністю";
            } catch (InterruptedException e) {
                sawCancel = true;
                return name + ": зупинилася на прохання";
            } finally {
                livedMs = System.currentTimeMillis() - t0;
                cleaned = true; // блок очищення: закриваємо за собою
            }
        }
    }

    /** Зайнятий потік без жодної перевірки прапорця перерви. */
    static void burn(long ms) {
        long end = System.nanoTime() + ms * 1_000_000L;
        while (System.nanoTime() < end) {
            /* тут мало б бути обчислення */ }
    }

    /** Чотири задачі прогону. Порядок важливий: та, що падає, стоїть першою. */
    static Job[] plan() {
        return new Job[] {
                new Job("ціна", Kind.FAILS, 100), // падає через 100 мс
                new Job("рецензії", Kind.SLEEPS, 2000), // просить 2000 мс, спить кроками по 200
                new Job("сусіди", Kind.SLEEPS, 2000),
                new Job("рейтинг", Kind.BURNS, 600) // 600 мс обчислень, нічого не перевіряє
        };
    }

    /** Обробити Future, спіймати падіння і попросити решту зупинитися. */
    static void waitAllAndCancel(Job[] jobs, ExecutorService pool) throws InterruptedException {
        List<Future<String>> futures = new ArrayList<>();
        for (Job j : jobs)
            futures.add(pool.submit(j));

        for (Future<String> f : futures) {
            try {
                System.out.println("  " + f.get());
            } catch (ExecutionException e) {
                System.out.println("  впала: " + e.getCause().getMessage());
                for (Future<String> rest : futures) {
                    if (!rest.isDone())
                        rest.cancel(true);
                }
            } catch (CancellationException e) {
                System.out.println("  задачу скасовано, результату немає");
            }
        }
    }

    // ---------------------------------------------------------- дано нижче

    /** Робочий приклад на двох задачах: перша падає, другу скасовуємо. */
    static void demo() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Job loser = new Job("демо-падає", Kind.FAILS, 100);
        Job other = new Job("демо-спить", Kind.SLEEPS, 2000);
        List<Future<String>> futures = List.of(pool.submit(loser), pool.submit(other));
        long t0 = System.currentTimeMillis();
        for (Future<String> f : futures) {
            try {
                System.out.println("  " + f.get());
            } catch (ExecutionException e) {
                System.out.println("  впала: " + e.getCause().getMessage());
                for (Future<String> rest : futures)
                    if (!rest.isDone())
                        rest.cancel(true);
            } catch (CancellationException e) {
                System.out.println("  задачу скасовано, результату немає");
            }
        }
        pool.shutdown();
        pool.awaitTermination(3, TimeUnit.SECONDS);
        System.out.println("  разом " + (System.currentTimeMillis() - t0) + " мс");
        System.out.println("  " + line(other));
    }

    /** Замір без скасування: чекаємо всіх і нікого не просимо зупинитися. */
    static void naive() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        Job[] jobs = plan();
        List<Future<String>> futures = new ArrayList<>();
        for (Job j : jobs)
            futures.add(pool.submit(j));
        long t0 = System.currentTimeMillis();
        for (Future<String> f : futures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                System.out.println("  впала: " + e.getCause().getMessage());
            }
        }
        pool.shutdown();
        System.out.println("  разом " + (System.currentTimeMillis() - t0) + " мс");
        for (Job j : jobs)
            System.out.println("  " + line(j));
    }

    /** Ваш варіант. */
    static void mine() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        Job[] jobs = plan();
        long t0 = System.currentTimeMillis();
        waitAllAndCancel(jobs, pool);
        System.out.println("  обробка Future: " + (System.currentTimeMillis() - t0) + " мс");
        pool.shutdown();
        if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Задачі не завершилися за 5 секунд");
        }
        System.out.println("  усі задачі завершились: " + (System.currentTimeMillis() - t0) + " мс");
        for (Job j : jobs)
            System.out.println("  " + line(j));
    }

    static String line(Job j) {
        return String.format("%-9s прожила %4d мс, прохання побачила: %-5b, очищення: %b",
                j.name, j.livedMs, j.sawCancel, j.cleaned);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("demo, дві задачі:");
        demo();
        System.out.println("\nбез скасування, чотири задачі:");
        naive();
        System.out.println("\nзі скасуванням, ті самі чотири задачі:");
        mine();
        System.out.println("\n«рейтинг» має дожити до кінця і прохання не побачити:");
        System.out.println("cancel(true) ставить прапорець, а не вбиває потік.");
    }
}
