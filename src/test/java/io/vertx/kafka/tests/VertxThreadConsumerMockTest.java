package io.vertx.kafka.tests;

import io.vertx.core.Vertx;
import io.vertx.kafka.KafkaConsumer;
import io.vertx.kafka.impl.VertxThreadConsumer;
import org.apache.kafka.clients.consumer.Consumer;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class VertxThreadConsumerMockTest extends ConsumerMockTestBase {

  @Override
  <K, V> KafkaConsumer<K, V> createConsumer(Vertx vertx, Consumer<K, V> consumer) {
    return VertxThreadConsumer.create(vertx, consumer);
  }
}
