package io.vertx.kafka.client;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(VertxUnitRunner.class)
public class RetryHelperTest {

  private Vertx vertx = Vertx.vertx();

  @After
  public void tearDown() {
    vertx.close().await();
  }

  @Test
  public void testEventualSuccess(TestContext ctx) {
    Async async = ctx.async();
    AtomicInteger attempts = new AtomicInteger(0);
    RetryHelper.forAction(vertx, () -> {
        if (attempts.incrementAndGet() < 3) {
          return Future.failedFuture("Try again");
        }
        return Future.succeededFuture("Done");
      })
      .every(Duration.ofMillis(50))
      .withTimeout(Duration.ofSeconds(1))
      .execute()
      .onComplete(ar -> {
        ctx.assertTrue(ar.succeeded());
        ctx.assertEquals("Done", ar.result());
        ctx.assertEquals(3, attempts.get());
        async.complete();
      });
  }

  @Test
  public void testTimeout(TestContext ctx) {
    Async async = ctx.async();
    RetryHelper.forAction(vertx, () -> Future.failedFuture("Persistent error"))
      .every(Duration.ofMillis(10))
      .withTimeout(Duration.ofMillis(100))
      .execute()
      .onComplete(ar -> {
        ctx.assertTrue(ar.failed());
        ctx.assertTrue(ar.cause().getMessage().equals("Timeout"));
        async.complete();
      });
  }

  @Test
  public void testCustomCondition(TestContext ctx) {
    Async async = ctx.async();
    AtomicInteger status = new AtomicInteger(0);
    RetryHelper.forAction(vertx, () -> Future.succeededFuture(status.incrementAndGet()))
      .until(ar -> ar.succeeded() && ar.result() == 3)
      .every(Duration.ofMillis(10))
      .execute()
      .onComplete(ar -> {
        ctx.assertTrue(ar.succeeded());
        ctx.assertEquals(3, ar.result());
        async.complete();
      });
  }
}
