import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.lang.Thread.sleep;

/**
 * In this chapter we are going to cover fundamentals of how to create a sequence. At the end of this
 * chapter we will tackle more complex methods like generate, create, push, and we will meet them again in following
 * chapters like Sinks and Backpressure.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#which.create
 * https://projectreactor.io/docs/core/release/reference/#producing
 * https://projectreactor.io/docs/core/release/reference/#_simple_ways_to_create_a_flux_or_mono_and_subscribe_to_it
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c5_CreatingSequence {

    /**
     * Emit value that you already have.
     */
    @Test
    public void value_I_already_have_mono() {
        String valueIAlreadyHave = "value";
        Mono<String> valueIAlreadyHaveMono = Mono.just(valueIAlreadyHave); //todo: change this line only

        StepVerifier.create(valueIAlreadyHaveMono)
                    .expectNext("value")
                    .verifyComplete();
    }

    /**
     * Emit potentially null value that you already have.
     */
    @Test
    public void potentially_null_mono() {
        String potentiallyNull = null;
        Mono<String> potentiallyNullMono = Mono.justOrEmpty(potentiallyNull); //todo change this line only

        StepVerifier.create(potentiallyNullMono)
                    .verifyComplete();
    }

    /**
     * Emit value from a optional.
     */
    @Test
    public void optional_value() {
        Optional<String> optionalValue = Optional.of("optional");
        Mono<String> optionalMono = Mono.justOrEmpty(optionalValue); //todo: change this line only

        StepVerifier.create(optionalMono)
                    .expectNext("optional")
                    .verifyComplete();
    }

    /**
     * Convert callable task to Mono.
     */
    @Test
    public void callable_counter() {
        AtomicInteger callableCounter = new AtomicInteger(0);
        Callable<Integer> callable = () -> {
            System.out.println("You are incrementing a counter via Callable!");
            return callableCounter.incrementAndGet();
        };

        Mono<Integer> callableCounterMono = Mono.fromCallable(callable); //todo: change this line only

        StepVerifier.create(callableCounterMono.repeat(2))
                    .expectNext(1, 2, 3)
                    .verifyComplete();
    }

    /**
     * Convert Future task to Mono.
     */
    @Test
    public void future_counter() {
        AtomicInteger futureCounter = new AtomicInteger(0);
        CompletableFuture<Integer> completableFuture = CompletableFuture.supplyAsync(() -> {
            System.out.println("You are incrementing a counter via Future!");
            return futureCounter.incrementAndGet();
        });
        Mono<Integer> futureCounterMono = Mono.fromFuture(completableFuture); //todo: change this line only

        StepVerifier.create(futureCounterMono)
                    .expectNext(1)
                    .verifyComplete();
    }

    /**
     * Convert Runnable task to Mono.
     */
    @Test
    public void runnable_counter() {
        AtomicInteger runnableCounter = new AtomicInteger(0);
        Runnable runnable = () -> {
            runnableCounter.incrementAndGet();
            System.out.println("You are incrementing a counter via Runnable!");
        };
        Mono<Integer> runnableMono = Mono.fromRunnable(runnable); //todo: change this line only

        StepVerifier.create(runnableMono.repeat(2))
                    .verifyComplete();

        Assertions.assertEquals(3, runnableCounter.get());
    }

    /**
     * Create Mono that emits no value but completes successfully.
     */
    @Test
    public void acknowledged() {
        Mono<String> acknowledged = Mono.empty(); //todo: change this line only

        StepVerifier.create(acknowledged)
                    .verifyComplete();
    }

    /**
     * Create Mono that emits no value and never completes.
     */
    @Test
    public void seen() {
        Mono<String> seen = Mono.never(); //todo: change this line only

        StepVerifier.create(seen.timeout(Duration.ofSeconds(5)))
                    .expectSubscription()
                    .expectNoEvent(Duration.ofSeconds(4))
                    .verifyTimeout(Duration.ofSeconds(5));
    }

    /**
     * Create Mono that completes exceptionally with exception `IllegalStateException`.
     */
    @Test
    public void trouble_maker() {
        Mono<String> trouble = Mono.error(new IllegalStateException()); //todo: change this line

        StepVerifier.create(trouble)
                    .expectError(IllegalStateException.class)
                    .verify();
    }

    /**
     * Create Flux that will emit all values from the array.
     */
    @Test
    public void from_array() {
        Integer[] array = {1, 2, 3, 4, 5};
        Flux<Integer> arrayFlux = Flux.just(array); //todo: change this line only

        //정답은
//        Flux<Integer> arrayFlux = Flux.fromArray(array); //todo: change this line only

        StepVerifier.create(arrayFlux)
                    .expectNext(1, 2, 3, 4, 5)
                    .verifyComplete();
    }

    /**
     * Create Flux that will emit all values from the list.
     */
    @Test
    public void from_list() {
        List<String> list = Arrays.asList("1", "2", "3", "4", "5");
        Flux<String> listFlux = Flux.fromIterable(list); //todo: change this line only

        StepVerifier.create(listFlux)
                    .expectNext("1", "2", "3", "4", "5")
                    .verifyComplete();
    }

    /**
     * Create Flux that will emit all values from the stream.
     */
    @Test
    public void from_stream() {
        Stream<String> stream = Stream.of("5", "6", "7", "8", "9");
        Flux<String> streamFlux = Flux.fromStream(stream); //todo: change this line only

        StepVerifier.create(streamFlux)
                    .expectNext("5", "6", "7", "8", "9")
                    .verifyComplete();
    }

    /**
     * Create Flux that emits number incrementing numbers at interval of 1 second.
     */
    @Test
    public void interval() {
        Flux<Long> interval = Flux.interval(Duration.ofSeconds(1)); //todo: change this line only

        System.out.println("Interval: ");
        StepVerifier.create(interval.take(3).doOnNext(System.out::println))
                    .expectSubscription()
                    .expectNext(0L)
                    .expectNoEvent(Duration.ofMillis(900))
                    .expectNext(1L)
                    .expectNoEvent(Duration.ofMillis(900))
                    .expectNext(2L)
                    .verifyComplete();
    }

    /**
     * Create Flux that emits range of integers from [-5,5].
     */
    @Test
    public void range() {
        Flux<Integer> range = Flux.range(-5,11); //todo: change this line only

        System.out.println("Range: ");
        StepVerifier.create(range.doOnNext(System.out::println))
                    .expectNext(-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5)
                    .verifyComplete();
    }

    /**
     * Create Callable that increments the counter and returns the counter value, and then use `repeat()` operator to create Flux that emits
     * values from 0 to 10.
     */
    @Test
    public void repeat() {
        AtomicInteger counter = new AtomicInteger(0);
        //모르겠음.
        Flux<Integer> repeated = Mono.fromCallable(counter::incrementAndGet).repeat(9); //todo: change this line

        System.out.println("Repeat: ");
        StepVerifier.create(repeated.doOnNext(System.out::println))
                    .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                    .verifyComplete();
    }


    //출처 : https://www.vinsguru.com/mono-vs-flux-project-reactor/
    @Test
    public void simple_test() throws InterruptedException {
        System.out.println("Starts");
        System.out.println("thread id" + Thread.currentThread().getId());
        //flux emits one element per second
        Flux<Character> flux =
                Flux.just('a', 'b', 'c', 'd')
                .log()
                .delayElements(Duration.ofSeconds(1));
        //Observer 1 - takes 500ms to process
        flux
                .map(Character::toUpperCase)
                .subscribe(i -> {
                    try {
                        sleep(500);
                        if(i == 'A'){
                            sleep(1000);
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println("Observer-1 : " + i + " thread id" + Thread.currentThread().getId());
                });
        //Observer 2 - process immediately
        flux.subscribe(i -> System.out.println("Observer-2 : " + i+ " thread id" + Thread.currentThread().getId()));

        System.out.println("Ends");

//Just to block the exe
        Thread.sleep(10000);
    }


    /**
     * Following example is just a basic usage of `generate,`create`,`push` sinks. We will learn how to use them in a
     * more complex scenarios when we tackle backpressure.
     *
     * Answer:
     * - What is difference between `generate` and `create`?
     *  generate는 sync 기반. create는 async + multi thread.
     * - What is difference between `create` and `push`?
     *  push는 단일 스레드 async. create는 멀티쓰레드.
     */




    @Test
    public void generate_programmatically() throws InterruptedException {

        //이 코드가 없었음.
        AtomicInteger counter = new AtomicInteger(0);

        Flux<Integer> generateFlux = Flux.generate(sink -> {
            try {
                sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if(counter.get() > 5) {
                sink.complete();
            }
            sink.next(counter.getAndIncrement());
            //todo: fix following code so it emits values from 0 to 5 and then completes
        });

        //generate test
//        generateFlux.subscribe(s -> System.out.println("1 thread id " + Thread.currentThread().getId() + " data: " + s));
//        //2번 째 거는 실행이 안됨. complete 가 발생했기 때문에 block 되어 그런가?
//        generateFlux.subscribe(s -> System.out.println("2 thread id " + Thread.currentThread().getId() + " data: " + s));
//        sleep(10000);


        //------------------------------------------------------
        AtomicInteger count = new AtomicInteger(0);
        /*atomic integer로 하면 안되는 거 같음.
        그냥 next가 호출이 안됨.
        subscribe를 하면 한 번만 호출하고 끝이남.
        자동으로 다음으로 넘어갈 생각이 없음.

        하지만 for 문으로 하면 잘됨.
        문서에 따르면, lambda 내에 블록킹 연산이 있으면 next 파이프라인이 잠길 수 있다고 함.
        (https://projectreactor.io/docs/core/release/reference/#_simple_ways_to_create_a_flux_or_mono_and_subscribe_to_it)
        즉, atomic 값을 증가시키려다가 잠기는 듯 함.
        */


        Flux<Integer> createFlux = Flux.create(sink -> {
//            if(count.get() > 5) {
//                sink.complete();
//            }
//            sink.next(count.getAndIncrement());


//            정답
            for(int i = 0; i <= 5; i++){
                sink.next(i);
            }
            sink.complete();
            //todo: fix following code so it emits values from 0 to 5 and then completes
        });

//        얘는 또 다 실행이 됨. 그리고 thread들이 같음..?
//        앞에서 just를 이용하여 flux를 만들면 다 같게 뜸.
//        thread가 같으니 sleep해서 async도 못봄

        System.out.println("create");
        createFlux.subscribe(s -> {
            try {
                sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println("1 thread id " + Thread.currentThread().getId() + " data: " + s);
        });

        createFlux.subscribe(s -> System.out.println("2 thread id " + Thread.currentThread().getId() + " data: " + s));
        sleep(5000);
        //------------------------------------------------------
        AtomicInteger countt = new AtomicInteger(0);

        Flux<Integer> pushFlux = Flux.push(sink -> {
//            if(countt.get() > 5) {
//                sink.complete();
//            }
//            sink.next(countt.getAndIncrement());

//                        정답
            for(int i = 0; i <= 5; i++){
                sink.next(i);
            }
            sink.complete();
            //todo: fix following code so it emits values from 0 to 5 and then completes
        });

        //create랑 같게 뜸. 흠.. create는 멀티쓰레드 아닌가?
        System.out.println("push");
        pushFlux.subscribe(s -> System.out.println("1 thread id " + Thread.currentThread().getId() + " data: " + s));
        pushFlux.subscribe(s -> System.out.println("2 thread id " + Thread.currentThread().getId() + " data: " + s));
        sleep(5000);

        StepVerifier.create(generateFlux)
                    .expectNext(0, 1, 2, 3, 4, 5)
                    .verifyComplete();

        //출력이 0, 1 이 됨. next가 제대로 안됨?
        //doOnNext도 출력이 안됨. 그냥 next가 호출이 안되는 거 같음.
        createFlux.doOnNext(s -> System.out.println("hi"));
        createFlux.subscribe(System.out::println);

        StepVerifier.create(createFlux)
                    .expectNext(0, 1, 2, 3, 4, 5)
                    .verifyComplete();
        System.out.println("test2");
        StepVerifier.create(pushFlux)
                    .expectNext(0, 1, 2, 3, 4, 5)
                    .verifyComplete();
        System.out.println("test3");
    }



    /**
     * Something is wrong with the following code. Find the bug and fix it so test passes.
     */
    @Test
    public void multi_threaded_producer() throws InterruptedException {
        //todo: find a bug and fix it!
        //Flux.create가 정답.
        //왜 push를 하면 무한 루프에 빠지는 지 모르겠음. 데드락 걸리면 아예 출력이 안되어야 하는데, 데이터 몇 개는 또 출력이 됨.
        Flux<Integer> producer = Flux.create(sink -> {
            for (int i = 0; i < 100; i++) {
                int finalI = i;

                new Thread(() -> sink.next(finalI)).start(); //don't change this line!
            }

        });

        //do not change code below
        StepVerifier.create(producer
                                    .doOnNext(System.out::println)
                                    .take(100))
                    .expectNextCount(100)
                    .verifyComplete();
    }
}
