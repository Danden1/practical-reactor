import org.junit.jupiter.api.*;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Backpressure is a mechanism that allows a consumer to signal to a producer that it is ready receive data.
 * This is important because the producer may be sending data faster than the consumer can process it, and can overwhelm consumer.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#reactive.backpressure
 * https://projectreactor.io/docs/core/release/reference/#_on_backpressure_and_ways_to_reshape_requests
 * https://projectreactor.io/docs/core/release/reference/#_operators_that_change_the_demand_from_downstream
 * https://projectreactor.io/docs/core/release/reference/#producing
 * https://projectreactor.io/docs/core/release/reference/#_asynchronous_but_single_threaded_push
 * https://projectreactor.io/docs/core/release/reference/#_a_hybrid_pushpull_model
 * https://projectreactor.io/docs/core/release/reference/#_an_alternative_to_lambdas_basesubscriber
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c10_Backpressure extends BackpressureBase {

    /**
     * In this exercise subscriber (test) will request several messages from the message stream.
     * Hook to the requests and record them to the `requests` list.
     */
    @Test
    public void request_and_demand() {
        CopyOnWriteArrayList<Long> requests = new CopyOnWriteArrayList<>();
        Flux<String> messageStream = messageStream1()
                .doOnRequest(requests::add)
                //todo: change this line only
                ;

        StepVerifier.create(messageStream, StepVerifierOptions.create().initialRequest(0))
                    .expectSubscription()
                    .thenRequest(1)
                    .then(() -> pub1.next("msg#1"))
                    .thenRequest(3)
                    .then(() -> pub1.next("msg#2", "msg#3"))
                    .then(pub1::complete)
                    .expectNext("msg#1", "msg#2", "msg#3")
                    .verifyComplete();

        Assertions.assertEquals(List.of(1L, 3L), requests);
    }

    /**
     * Adjust previous solution in such a way that you limit rate of requests. Number of requested messages stays the
     * same, but each request should be limited to 1 message.
     */
    @Test
    public void limited_demand() {
        CopyOnWriteArrayList<Long> requests = new CopyOnWriteArrayList<>();

        //limitRate가 앞에 오면 안됨. 왜지?
        //앞에 있으면 기존 flux 에 limit rate가 걸리고 그 다음 doOnRequest를 등록하면 되지 않나?
        //앞에 있으면 기존 flux에 limitRate가 걸리지 않나봄. 확인필요

        //limitRate하면, publishOn이 적용이 됨. 이 부분이 먼저 나오면 적용이 안되나 봄?
        //publishOn 부분 그대로 복사해서 앞에 적용해도 안됨.
        //TODO: publishOn 집에서 공부해야 할 듯.

        /*
        정답 로그
        [ INFO] (main) | onSubscribe([Fuseable] FluxPublishOn.PublishOnSubscriber)
        [ INFO] (main) | request(1)
        [ INFO] (main) | onNext(msg#1)
        [ INFO] (main) | request(3)
        [ INFO] (main) | onNext(msg#2)
        [ INFO] (main) | onNext(msg#3)
        [ INFO] (main) | onComplete()

        틀린 로그
        [ INFO] (main) | onSubscribe([Fuseable] FluxPeekFuseable.PeekFuseableSubscriber)
        [ INFO] (main) | request(1)
        [ INFO] (main) | onNext(msg#1)
        [ INFO] (main) | request(3)
        [ INFO] (main) | onNext(msg#2)
        [ INFO] (main) | onNext(msg#3)
        [ INFO] (main) | onComplete

        subcribe의 스케줄러가 다르긴함.
        이거 떄문에 문제 발생한 듯.
         */


        Flux<String> messageStream = messageStream2().doOnRequest(requests::add).limitRate(1).log()
                //todo: do your changes here
                ;



        StepVerifier.create(messageStream, StepVerifierOptions.create().initialRequest(0))
                    .expectSubscription()
                    .thenRequest(1)
                    .then(() -> pub2.next("msg#1"))
                    .thenRequest(3)
                    .then(() -> pub2.next("msg#2", "msg#3"))
                    .then(pub2::complete)
                    .expectNext("msg#1", "msg#2", "msg#3")
                    .verifyComplete();

        Assertions.assertEquals(List.of(1L, 1L, 1L, 1L), requests);
    }

    /**
     * Finish the implementation of the `uuidGenerator` so it exactly requested amount of UUIDs. Or better said, it
     * should respect the backpressure of the consumer.
     */
    @Test
    public void uuid_generator() {
        Flux<UUID> uuidGenerator = Flux.create(sink ->
            //todo: do your changes here
                {
                    sink.onRequest(req -> {
                        for (int i = 0; i < req; i++) {
                            sink.next(UUID.randomUUID());
                        }
                    });
//                    테스트 부분에 thenCancel()이 있어서 무한 루프 돌지 않는 듯.
//                    sink.complete();

                }
        );


        StepVerifier.create(uuidGenerator
                                    .doOnNext(System.out::println)
                                    .timeout(Duration.ofSeconds(1))
                                    .onErrorResume(TimeoutException.class, e -> Flux.empty()),
                            StepVerifierOptions.create().initialRequest(0))
                    .expectSubscription()
                    .thenRequest(10)
                    .expectNextCount(10)
                    .thenCancel()
                    .verify();
    }

    /**
     * You are receiving messages from malformed publisher that may not respect backpressure.
     * In case that publisher produces more messages than subscriber is able to consume, raise an error.
     */
    @Test
    public void pressure_is_too_much() {
        Flux<String> messageStream = messageStream3().onBackpressureError().log()
                //todo: change this line only
                ;

        StepVerifier.create(messageStream, StepVerifierOptions.create()
                                                              .initialRequest(0))
                    .expectSubscription()
                    .thenRequest(3)
                    .then(() -> pub3.next("A", "B", "C", "D"))
                    .expectNext("A", "B", "C")
                    .expectErrorMatches(Exceptions::isOverflow)
                    .verify();
    }

    /**
     * You are receiving messages from malformed publisher that may not respect backpressure. In case that publisher
     * produces more messages than subscriber is able to consume, buffer them for later consumption without raising an
     * error.
     */
    @Test
    public void u_wont_brake_me() {
        Flux<String> messageStream = messageStream4()
                //todo: change this line only
                ;

        StepVerifier.create(messageStream, StepVerifierOptions.create()
                                                              .initialRequest(0))
                    .expectSubscription()
                    .thenRequest(3)
                    .then(() -> pub4.next("A", "B", "C", "D"))
                    .expectNext("A", "B", "C")
                    .then(() -> pub4.complete())
                    .thenAwait()
                    .thenRequest(1)
                    .expectNext("D")
                    .verifyComplete();
    }

    /**
     * We saw how to react to request demand from producer side. In this part we are going to control demand from
     * consumer side by implementing BaseSubscriber directly.
     * Finish implementation of base subscriber (consumer of messages) with following objectives:
     * - once there is subscription, you should request exactly 10 messages from publisher
     * - once you received 10 messages, you should cancel any further requests from publisher.
     * Producer respects backpressure.
     */
    @Test
    public void subscriber() throws InterruptedException {
        AtomicReference<CountDownLatch> lockRef = new AtomicReference<>(new CountDownLatch(1));
        AtomicInteger count = new AtomicInteger(0);
        AtomicReference<Subscription> sub = new AtomicReference<>();

        remoteMessageProducer()
                .doOnCancel(() -> lockRef.get().countDown())
                .subscribeWith(new BaseSubscriber<String>() {
                    //todo: do your changes only within BaseSubscriber class implementation
                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        sub.set(subscription);
                    }

                    @Override
                    protected void hookOnNext(String s) {
                        System.out.println(s);
                        count.incrementAndGet();
                    }
                    //-----------------------------------------------------
                });

        lockRef.get().await();
        Assertions.assertEquals(10, count.get());
    }
}
