package io.vertx.kafka.tests;

import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.consumer.KafkaReadStream;
import io.vertx.kafka.producer.KafkaWriteStream;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class KafkaTestBase {

  static void close(TestContext ctx, KafkaReadStream<?, ?> consumer) {
    if (consumer != null) {
      Async close = ctx.async();
      consumer.close(v -> {
        close.complete();
      });
      close.awaitSuccess(10000);
    }
  }


  static <K, V> KafkaWriteStream<K, V> producer(Consumer<Future<KafkaWriteStream<K, V>>> builder) throws Exception {
    CompletableFuture<KafkaWriteStream<K, V>> fut = new CompletableFuture<>();
    builder.accept(Future.<KafkaWriteStream<K, V>>future().setHandler(ar -> {
      if (ar.succeeded()) {
        fut.complete(ar.result());
      } else {
        fut.completeExceptionally(ar.cause());
      }
    }));
    return fut.get(10, TimeUnit.SECONDS);
  }
}
