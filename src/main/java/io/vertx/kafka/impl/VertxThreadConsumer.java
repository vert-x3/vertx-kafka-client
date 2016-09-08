package io.vertx.kafka.impl;

import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Map;
import java.util.Properties;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class VertxThreadConsumer<K, V> extends KafkaConsumerBase<K, V> {

  public static <K, V> KafkaConsumerBase<K, V> create(Vertx vertx, Properties config) {
    return new VertxThreadConsumer<>(vertx.getOrCreateContext(), new org.apache.kafka.clients.consumer.KafkaConsumer<>(config));
  }


  public static <K, V> KafkaConsumerBase<K, V> create(Vertx vertx, Map<String, Object> config) {
    return new VertxThreadConsumer<>(vertx.getOrCreateContext(), new org.apache.kafka.clients.consumer.KafkaConsumer<>(config));
  }

  public static <K, V> KafkaConsumerBase<K, V> create(Vertx vertx, Consumer<K, V> consumer) {
    return new VertxThreadConsumer<>(vertx.getOrCreateContext(), consumer);
  }

  private VertxThreadConsumer(Context context, Consumer<K, V> consumer) {
    super(context, consumer);
  }


  @Override
  protected ConsumerRecords<K, V> fetchRecords() {
    return consumer.poll(0);
  }

  @Override
  protected void doClose(Handler<Void> completionHandler) {
    context.runOnContext(v -> {
      consumer.close();
      if (completionHandler != null) {
        completionHandler.handle(null);
      }
    });
  }
}
