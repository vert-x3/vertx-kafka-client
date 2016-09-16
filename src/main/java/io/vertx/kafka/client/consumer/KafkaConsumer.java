package io.vertx.kafka.client.consumer;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.ReadStream;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
@VertxGen
public interface KafkaConsumer extends ReadStream<KafkaConsumerRecord> {

  static KafkaConsumer create(Vertx vertx, ConsumerOptions options, Map<String, String> config) {
    KafkaReadStream<?, ?> stream = KafkaReadStream.create(vertx, options, new HashMap<>(config));
    return new KafkaConsumerImpl(stream);
  }

  @Override
  KafkaConsumer exceptionHandler(Handler<Throwable> handler);

  @Override
  KafkaConsumer handler(Handler<KafkaConsumerRecord> handler);

  @Override
  KafkaConsumer pause();

  @Override
  KafkaConsumer resume();

  @Override
  KafkaConsumer endHandler(Handler<Void> endHandler);

  KafkaConsumer subscribe(Set<String> topics);

  KafkaConsumer subscribe(Set<String> topics, Handler<AsyncResult<Void>> handler);

  void commit();

  void commit(Handler<AsyncResult<Void>> completionHandler);

  default void close() {
    close(null);
  }

  void close(Handler<Void> completionHandler);

}
