package io.vertx.kafka.client;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Helper for retrying asynchronous Vert.x actions until a condition is met or a timeout is reached.
 */
public class RetryHelper<T> {

  private final Vertx vertx;
  private final Supplier<Future<T>> action;

  private Function<AsyncResult<T>, Boolean> condition = AsyncResult::succeeded;
  private Duration timeout = Duration.ofSeconds(5);
  private Duration interval = Duration.ofMillis(500);

  private RetryHelper(Vertx vertx, Supplier<Future<T>> action) {
    this.vertx = Objects.requireNonNull(vertx, "Vertx instance cannot be null");
    this.action = Objects.requireNonNull(action, "Action supplier cannot be null");
  }

  /**
   * Create a new RetryHelper with a mandatory action.
   *
   * @param vertx  the Vert.x instance used to schedule retries
   * @param action a supplier of asynchronous actions
   */
  public static <T> RetryHelper<T> forAction(Vertx vertx, Supplier<Future<T>> action) {
    return new RetryHelper<>(vertx, action);
  }

  /**
   * Set a condition that stops the retry loop. It defaults to {@link AsyncResult::succeeded}.
   *
   * @param condition a function that evaluates the result of an attempt, returning {@code true} to stop retrying
   */
  public RetryHelper<T> until(Function<AsyncResult<T>, Boolean> condition) {
    this.condition = Objects.requireNonNull(condition, "Condition function cannot be null");
    return this;
  }

  /**
   * Sets the maximum duration to keep retrying. Defaults to 5 seconds.
   */
  public RetryHelper<T> withTimeout(Duration timeout) {
    this.timeout = Objects.requireNonNull(timeout, "Timeout duration cannot be null");
    return this;
  }

  /**
   * Sets the delay between retries. Defaults to 500 milliseconds.
   */
  public RetryHelper<T> every(Duration interval) {
    this.interval = Objects.requireNonNull(interval, "Interval duration cannot be null");
    return this;
  }

  /**
   * Starts the retry loop.
   *
   * @return a {@link Future} that completes when the condition is met or the timeout expires.
   */
  public Future<T> execute() {
    Promise<T> promise = Promise.promise();
    Instant deadline = Instant.now().plus(timeout);
    attempt(deadline, promise);
    return promise.future();
  }

  private void attempt(Instant deadline, Promise<T> promise) {
    action.get().onComplete(ar -> {
      if (condition.apply(ar)) {
        promise.handle(ar);
        return;
      }
      if (Instant.now().isAfter(deadline)) {
        promise.fail("Timeout");
        return;
      }
      vertx.setTimer(interval.toMillis(), id -> attempt(deadline, promise));
    });
  }
}
