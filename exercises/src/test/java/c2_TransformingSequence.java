import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * It's time to do some data manipulation!
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#which.values
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c2_TransformingSequence extends TransformingSequenceBase {

    /***
     * Your task is simple:
     *  Increment each number emitted by the numerical service
     */
    @Test
    public void transforming_sequence() {
        Flux<Integer> numbersFlux = numerical_service()
                .map(s->s+1)
                //todo change only this line
                ;

        //StepVerifier is used for testing purposes
        //ignore it for now, or explore it independently
        StepVerifier.create(numbersFlux)
                    .expectNext(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
                    .verifyComplete();
    }

    /***
     * Transform given number sequence to:
     *   - ">": if given number is greater than 0
     *   - "=": if number is equal to 0
     *   - "<": if given number is lesser then 0
     */
    @Test
    public void transforming_sequence_2() {
        Flux<Integer> numbersFlux = numerical_service_2();

        //todo: do your changes here
        Flux<String> resultSequence = numbersFlux.flatMap(s-> {
            if( s > 0){
                return Flux.just(">");
            }
            else if(s < 0){
                return Flux.just("<");
            }
            else{
                return Flux.just("=");
            }
        });

        //정답은
        /*
        Flux<String> resultSequence = numbersFlux
                .map(i -> {
                    if (i > 0) {
                        return ">";
                    } else if (i == 0) {
                        return "=";
                    } else {
                        return "<";
                    }
            });
         */

        //하지만 flatMap이 비동기라서 flux -> flux는 이게 좋다고 하는 거 같은데?
        //만약 비동기면, sleep이 없어도 이상하게 테스트 통과가 됨.
        //map은 동기, flatMap은 async로 작동한다고 함. 근데 왜 테스트는...?

        //don't change code below
        StepVerifier.create(resultSequence)
                    .expectNext(">", "<", "=", ">", ">")
                    .verifyComplete();
    }

    /**
     * `object_service()` streams sequence of Objects, but if you peek into service implementation, you can see
     * that these items are in fact strings!
     * Casting using `map()` to cast is one way to do it, but there is more convenient way.
     * Remove `map` operator and use more appropriate operator to cast sequence to String.
     */
    @Test
    public void cast() {
        Flux<String> numbersFlux = object_service()
                .cast(String.class); //todo: change this line only


        StepVerifier.create(numbersFlux)
                    .expectNext("1", "2", "3", "4", "5")
                    .verifyComplete();
    }

    /**
     * `maybe_service()` may return some result.
     * In case it doesn't return any result, return value "no results".
     */
    @Test
    public void maybe() {
        Mono<String> result = maybe_service()
                .defaultIfEmpty("no results")
                //todo: change this line only
                ;

        StepVerifier.create(result)
                    .expectNext("no results")
                    .verifyComplete();
    }

    /**
     * Reduce the values from `numerical_service()` into a single number that is equal to sum of all numbers emitted by
     * this service.
     */
    @Test
    public void sequence_sum() {
        //todo: change code as you need
        Mono<Integer> sum = numerical_service().reduce(0, Integer::sum);
        //reduce 소스코드 보면, mono를 return하게 되어 있음.
        //무슨 메소드를 호출해야할 지 모르겠는데, 알고보니 주석에 나와있음.

        //don't change code below
        StepVerifier.create(sum)
                    .expectNext(55)
                    .verifyComplete();
    }

    /***
     *  Reduce the values from `numerical_service()` but emit each intermediary number
     *  Use first Flux value as initial value.
     */
    @Test
    public void sum_each_successive() {
        Flux<Integer> sumEach = numerical_service().scan(Integer::sum)
                //todo: do your changes here
                ;
        sumEach.subscribe(System.out::println);
        //scan(0, Integer::sum)은 안됨. 0이 먼저 인자로 들어감.

        StepVerifier.create(sumEach)
                    .expectNext(1, 3, 6, 10, 15, 21, 28, 36, 45, 55)
                    .verifyComplete();
    }

    /**
     * A developer who wrote `numerical_service()` forgot that sequence should start with zero, so you must prepend zero
     * to result sequence.
     *
     * Do not alter `numerical_service` implementation!
     * Use only one operator.
     */
    @Test
    public void sequence_starts_with_zero() {
        Flux<Integer> result = numerical_service().startWith(0)
                //todo: change this line only
                ;

        StepVerifier.create(result)
                    .expectNext(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                    .verifyComplete();
    }
}
