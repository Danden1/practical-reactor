import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * It's time introduce some resiliency by recovering from unexpected events!
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#which.errors
 * https://projectreactor.io/docs/core/release/reference/#error.handling
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c7_ErrorHandling extends ErrorHandlingBase {

    /**
     * You are monitoring hearth beat signal from space probe. Heart beat is sent every 1 second.
     * Raise error if probe does not any emit heart beat signal longer then 3 seconds.
     * If error happens, save it in `errorRef`.
     */
    @Test
    public void houston_we_have_a_problem() {
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        Flux<String> heartBeat = probeHeartBeatSignal().timeout(Duration.ofSeconds(3))
                .doOnError(errorRef::set)
                //todo: do your changes here
                //todo: & here
                ;

        StepVerifier.create(heartBeat)
                    .expectNextCount(3)
                    .expectError(TimeoutException.class)
                    .verify();

        Assertions.assertTrue(errorRef.get() instanceof TimeoutException);
    }

    /**
     * Retrieve currently logged user.
     * If any error occurs, exception should be further propagated as `SecurityException`.
     * Keep original cause.
     */
    @Test
    public void potato_potato() {
        Mono<String> currentUser = getCurrentUser().onErrorMap(SecurityException::new)
                //todo: change this line only
                //use SecurityException
                ;

        StepVerifier.create(currentUser)
                    .expectErrorMatches(e -> e instanceof SecurityException &&
                            e.getCause().getMessage().equals("No active session, user not found!"))
                    .verify();
    }

    /**
     * Consume all the messages `messageNode()`.
     * Ignore any failures, and if error happens finish consuming silently without propagating any error.
     */
    @Test
    public void under_the_rug() {
        Flux<String> messages = messageNode().onErrorResume(s-> Mono.empty());
        //todo: change this line only
        ;

        StepVerifier.create(messages)
                    .expectNext("0x1", "0x2")
                    .verifyComplete();
    }

    /**
     * Retrieve all the messages `messageNode()`,and if node suddenly fails
     * use `backupMessageNode()` to consume the rest of the messages.
     */
    @Test
    public void have_a_backup() {
        //todo: feel free to change code as you need
        Flux<String> messages = messageNode().onErrorResume(s -> backupMessageNode());

        //don't change below this line
        StepVerifier.create(messages)
                    .expectNext("0x1", "0x2", "0x3", "0x4")
                    .verifyComplete();
    }

    /**
     * Consume all the messages `messageNode()`, if node suddenly fails report error to `errorReportService` then
     * propagate error downstream.
     */
    @Test
    public void error_reporter() {
        //todo: feel free to change code as you need

        //errorReportService가 호출이 되지 않아서 errorReported()의 값이 설정되지 않음.
        //하지만 Mono<String> 형태를 반환하나봄.... 왜지... 자바를 좀 더 공부해야하나(특히 generic)
//        Flux<String> messages = messageNode().onErrorResume(s->Mono.error(s));

        Flux<String> messages = messageNode().onErrorResume(s->errorReportService(s).then(Mono.error(s)));

        //don't change below this line
        StepVerifier.create(messages)
                    .expectNext("0x1", "0x2")
                    .expectError(RuntimeException.class)
                    .verify();
        Assertions.assertTrue(errorReported.get());
    }

    /**
     * Execute all tasks from `taskQueue()`. If task executes
     * without any error, commit task changes, otherwise rollback task changes.
     * Do don't propagate any error downstream.
     */
    @Test
    public void unit_of_work() {
        //map 은 새로운 publisher를 만드는 것? subscribe는 단순히 소비? 흠
        Flux<Task> taskFlux = taskQueue().flatMap(s->
            s.execute()
            .then(s.commit())
            .onErrorResume(s::rollback)
            .thenReturn(s)
        )
                //todo: do your changes here
                ;

        StepVerifier.create(taskFlux)
                    .expectNextMatches(task -> task.executedExceptionally.get() && !task.executedSuccessfully.get())
                    .expectNextMatches(task -> task.executedSuccessfully.get() && task.executedSuccessfully.get())
                    .verifyComplete();
    }

    /**
     * `getFilesContent()` should return files content from three different files. But one of the files may be
     * corrupted and will throw an exception if opened.
     * Using `onErrorContinue()` skip corrupted file and get the content of the other files.
     */
    @Test
    public void billion_dollar_mistake() {
        Flux<String> content = getFilesContent()
                .flatMap(Function.identity())
                .onErrorContinue((e,o) -> {})
                //todo: change this line only
                ;

        StepVerifier.create(content)
                    .expectNext("file1.txt content", "file3.txt content")
                    .verifyComplete();
    }

    /**
     * Quote from one of creators of Reactor: onErrorContinue is my billion-dollar mistake. `onErrorContinue` is
     * considered as a bad practice, its unsafe and should be avoided.
     *
     * {@see <a href="https://nurkiewicz.com/2021/08/onerrorcontinue-reactor.html">onErrorContinue</a>} {@see <a
     * href="https://devdojo.com/ketonemaniac/reactor-onerrorcontinue-vs-onerrorresume">onErrorContinue vs
     * onErrorResume</a>} {@see <a href="https://bsideup.github.io/posts/daily_reactive/where_is_my_exception/">Where is
     * my exception?</a>}
     *
     * Your task is to implement `onErrorContinue()` behaviour using `onErrorResume()` operator,
     * by using knowledge gained from previous lessons.
     */
    @Test
    public void resilience() throws InterruptedException {
        //todo: change code as you need
        Flux<String> content = getFilesContent()
                .flatMap(s->s.onErrorResume(e->Mono.empty()))
        // .onErrorResume(...) 여기로 옮기면 안됨. 파일 2에서 예외가 발생하고, 파일3으로 넘어가지 않는 현상이 발생함.
        // 그 이유는 s는 mono임. 즉, mono에서 error가 뜨면, 단순히 그 부분을 empty Mono로 대체하고 다음 Mono를 실행하게 됨.
        // 하지만 flux에 onErrorResume을 하게 된다면, 다음인 파일 3으로 넘어가지 않음. 파일 2에서 에러가 떴기 때문에 이를 빈 mono가 대체하게 됨.

                ; //start from here

        //don't change below this line
        StepVerifier.create(content)
                    .expectNext("file1.txt content", "file3.txt content")
                    .verifyComplete();

    }

    /**
     * You are trying to read temperature from your recently installed DIY IoT temperature sensor. Unfortunately, sensor
     * is cheaply made and may not return value on each read. Keep retrying until you get a valid value.
     */
    @Test
    public void its_hot_in_here() {
        Mono<Integer> temperature = temperatureSensor().retry();

                //todo: change this line only
                ;

        StepVerifier.create(temperature)
                    .expectNext(34)
                    .verifyComplete();
    }

    /**
     * In following example you are trying to establish connection to database, which is very expensive operation.
     * You may retry to establish connection maximum of three times, so do it wisely!
     * FIY: database is temporarily down, and it will be up in few seconds (5).
     */
    @Test
    public void back_off() {
        Mono<String> connection_result = establishConnection()
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(5)))
                //todo: change this line only
                ;

        StepVerifier.create(connection_result)
                    .expectNext("connection_established")
                    .verifyComplete();
    }

    /**
     * You are working with legacy system in which you need to read alerts by pooling SQL table. Implement polling
     * mechanism by invoking `nodeAlerts()` repeatedly until you get all (2) alerts. If you get empty result, delay next
     * polling invocation by 1 second.
     */
    @Test
    public void good_old_polling() {
        //todo: change code as you need
        //empty일 때 이 안의 publisher가 실행됨.
        //즉 비어있으면 몇 초 기다리거나 이런 식으로 수행할 수 있음. 근데 데이터 들어와서 읽게 되면?
        //complete되고 종료되는 거 같음.
        Flux<String> alerts = nodeAlerts().repeatWhenEmpty(it -> it.delayElements(Duration.ofSeconds(5)))
                .repeat();
        ;
        nodeAlerts();

        //don't change below this line
        StepVerifier.create(alerts.take(2))
                    .expectNext("node1:low_disk_space", "node1:down")
                    .verifyComplete();
    }

    public static class SecurityException extends Exception {

        public SecurityException(Throwable cause) {
            super(cause);
        }
    }
}
