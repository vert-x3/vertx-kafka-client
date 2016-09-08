package io.vertx.kafka.tests;

import io.vertx.core.Future;
import io.vertx.kafka.KafkaProducer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class KafkaTestBase {

  static <K, V> KafkaProducer<K, V> producer(Consumer<Future<KafkaProducer<K, V>>> builder) throws Exception {
    CompletableFuture<KafkaProducer<K, V>> fut = new CompletableFuture<>();
    builder.accept(Future.<KafkaProducer<K, V>>future().setHandler(ar -> {
      if (ar.succeeded()) {
        fut.complete(ar.result());
      } else {
        fut.completeExceptionally(ar.cause());
      }
    }));
    return fut.get(10, TimeUnit.SECONDS);
  }
}
