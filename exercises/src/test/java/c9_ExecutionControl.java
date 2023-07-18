import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import reactor.blockhound.BlockHound;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.NonBlocking;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * With multi-core architectures being a commodity nowadays, being able to easily parallelize work is important.
 * Reactor helps with that by providing many mechanisms to execute work in parallel.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#schedulers
 * https://projectreactor.io/docs/core/release/reference/#advanced-parallelizing-parralelflux
 * https://projectreactor.io/docs/core/release/reference/#_the_publishon_method
 * https://projectreactor.io/docs/core/release/reference/#_the_subscribeon_method
 * https://projectreactor.io/docs/core/release/reference/#which.time
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */




public class c9_ExecutionControl extends ExecutionControlBase {
    /**
     * You are working on smartphone app and this part of code should show user his notifications. Since there could be
     * multiple notifications, for better UX you want to slow down appearance between notifications by 1 second.
     * Pay attention to threading, compare what code prints out before and after solution. Explain why?
     */
    @Test
    public void slow_down_there_buckaroo() {
        long threadId = Thread.currentThread().getId();
        Flux<String> notifications = readNotifications()
                .doOnNext(s -> {
                    System.out.println(s);
                    //여기서는 같은 쓰레드임.
//                    System.out.println(threadId);
//                    System.out.println(Thread.currentThread().getId());
                })
                //왜 다른 thread에서 작동하지?
                //main thread와 다른 곳에서 작동함.
                //default로 parallel 스케쥴러로 작동하기 때문에.
                //mono가 호출되어 있는 thread id를 따라가는 듯 함.
                //모든 원소를 parallel 하게 publish해서 그런가봄.
                .delayElements(Duration.ofSeconds(1))

                //todo: change this line only
                ;

        StepVerifier.create(notifications
                                    .doOnNext(s -> {
                                        assertThread(threadId);
                                        System.out.println(Thread.currentThread().getId());
                                    }))
                    .expectNextCount(5)
                    .verifyComplete();
    }

    private void assertThread(long invokerThreadId) {
        long currentThread = Thread.currentThread().getId();
        if (currentThread != invokerThreadId) {
            System.out.println("-> Not on the same thread");
        } else {
            System.out.println("-> On the same thread");
        }
        Assertions.assertTrue(currentThread != invokerThreadId, "Expected to be on a different thread");
    }

    /**
     * You are using free access to remote hosting machine. You want to execute 3 tasks on this machine, but machine
     * will allow you to execute one task at a time on a given schedule which is orchestrated by the semaphore. If you
     * disrespect schedule, your access will be blocked.
     * Delay execution of tasks until semaphore signals you that you can execute the task.
     */
    @Test
    public void ready_set_go() {
        //todo: feel free to change code as you need
        Flux<String> tasks = tasks()
                .concatMap(s -> s.delaySubscription(semaphore()));


        //don't change code below
        StepVerifier.create(tasks)
                    .expectNext("1")
                    .expectNoEvent(Duration.ofMillis(2000))
                    .expectNext("2")
                    .expectNoEvent(Duration.ofMillis(2000))
                    .expectNext("3")
                    .verifyComplete();
    }

    /**
     * Make task run on thread suited for short, non-blocking, parallelized work.
     * Answer:
     * - Which types of schedulers Reactor provides?
     * - What is their purpose?
     * - What is their difference?
     */
    @Test
    public void non_blocking() {
        //여기서는 publishOn이랑 subscribeOn 전부 잘됨.
        //publishOn은 downstream에서, subscribeOn은 upstream.
        //뒤 예제들 더 풀어봐야 알 듯 함.

        // 여기서는 pa가 출력이 됨.(publishOn)
        // 하지만 subscribeOn과 publishOn의 위치를 바꾸면 sa가 출력이 됨. 왜 그러지? 모르겠는데...

        //new가 붙어있는 것들은 thread를 따로 지워줘야함? -> 개수 똑같은데? 시간이 지나면 모르겠음. 시간 지날 시에 남은 thread가 지장을 줄 수도?
        Mono<Void> task = Mono.fromRunnable(() -> {
                                  Thread currentThread = Thread.currentThread();
                                  assert NonBlocking.class.isAssignableFrom(Thread.currentThread().getClass());
                                  System.out.println("Task executing on: " + currentThread.getName());
                              })
                .publishOn(Schedulers.newParallel("pa"))
                .subscribeOn(Schedulers.newParallel("sa"))
                              //todo: change this line only
                              .then();

        StepVerifier.create(task)
                    .verifyComplete();
        System.out.println(java.lang.Thread.activeCount());

    }

    /**
     * Make task run on thread suited for long, blocking, parallelized work.
     * Answer:
     * - What BlockHound for?
     */
    @Test
    public void blocking() {
        BlockHound.install(); //don't change this line
        //jvm 문제로 안됨.
        Mono<Void> task = Mono.fromRunnable(ExecutionControlBase::blockingCall)
                              .subscribeOn(Schedulers.boundedElastic())//todo: change this line only
                              .then();

        StepVerifier.create(task)
                    .verifyComplete();
    }

    /**
     * Adapt code so tasks are executed in parallel, with max concurrency of 3.
     */
    @Test
    public void free_runners() {
        //todo: feel free to change code as you need

        //여기도 둘 다 됨. subOn 이랑 pubOn.
        Mono<Void> task = Mono.fromRunnable(ExecutionControlBase::blockingCall).publishOn((Schedulers.boundedElastic())).then();


        //concatMap 앞의 처리가 완료될 때까지 기다리기 때문에 flatMap을 써야함.
        //flatMapSequential은 됨. 그 이유는 concatMap은 publish한 데이터의 처리가 끝나고 다음으로 넘어가기 때문임. 근데 async라고 함.
        Flux<Void> taskQueue = Flux.just(task, task, task)
                                   .flatMapSequential(Function.identity(),3);


        //don't change code below
        Duration duration = StepVerifier.create(taskQueue)
                                        .expectComplete()
                                        .verify();

        Assertions.assertTrue(duration.getSeconds() <= 2, "Expected to complete in less than 2 seconds");
    }

    /**
     * Adapt the code so tasks are executed in parallel, but task results should preserve order in which they are invoked.
     */
    @Test
    public void sequential_free_runners() {
        //todo: feel free to change code as you need
        Flux<String> tasks = tasks()
                .flatMapSequential(Function.identity())
                //없어도 되네?
//                .publishOn(Schedulers.parallel())
        ;


        //위의 주석을 풀어도 똑같이 4개를 사용함.
        //cpu 코어 만큼 만든다며?
        System.out.println(Thread.activeCount());

        //don't change code below
        Duration duration = StepVerifier.create(tasks)
                                        .expectNext("1")
                                        .expectNext("2")
                                        .expectNext("3")
                                        .verifyComplete();

        Assertions.assertTrue(duration.getSeconds() <= 1, "Expected to complete in less than 1 seconds");
    }

    /**
     * Make use of ParallelFlux to branch out processing of events in such way that:
     * - filtering events that have metadata, printing out metadata, and mapping to json can be done in parallel.
     * Then branch in before appending events to store. `appendToStore` must be invoked sequentially!
     */
    @Test
    public void event_processor() {
        //todo: feel free to change code as you need
        //모르겠음.
        //코드가 어떻게 작동하는 지는 알겠음.
        //sequentail()로 parallel한 것을 합침. -> 이 과정 blocking 아닌가?

        //parallel은 단순히 나누기만 함. 이를 병렬로 실행하기 위해서는 runon이 필요함.
        Flux<String> eventStream = eventProcessor()
                .parallel()
                .runOn(Schedulers.parallel())
                .filter(event -> event.metaData.length() > 0)
                .doOnNext(event -> System.out.println("Mapping event: " + event.metaData))
                .map(this::toJson)
                .sequential()
                .concatMap(n -> {

                    System.out.println(Thread.currentThread().getId());
                    return appendToStore(n).thenReturn(n);
                });


        //concatMap은 parallel 하지 않음?
        //흠...
//                .filter(event -> event.metaData.length() > 0)
//                .doOnNext(event -> System.out.println("Mapping event: " + event.metaData))
//                .map(this::toJson)
//                .publishOn(Schedulers.parallel())
//                .concatMap(n -> {System.out.println(Thread.currentThread().getId());
//                    return appendToStore(n).thenReturn(n);});

        //thread 3개?
        System.out.println(Thread.activeCount());
        //don't change code below
        StepVerifier.create(eventStream)
                    .expectNextCount(250)
                    .verifyComplete();

        List<String> steps = Scannable.from(eventStream)
                                      .parents()
                                      .map(Object::toString)
                                      .collect(Collectors.toList());

        String last = Scannable.from(eventStream)
                               .steps()
                               .collect(Collectors.toCollection(LinkedList::new))
                               .getLast();

        Assertions.assertEquals("concatMap", last);
        Assertions.assertTrue(steps.contains("ParallelMap"), "Map operator not executed in parallel");
        Assertions.assertTrue(steps.contains("ParallelPeek"), "doOnNext operator not executed in parallel");
        Assertions.assertTrue(steps.contains("ParallelFilter"), "filter operator not executed in parallel");
        Assertions.assertTrue(steps.contains("ParallelRunOn"), "runOn operator not used");
    }

    private String toJson(Event n) {
        try {
            return new ObjectMapper().writeValueAsString(n);
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }
}
