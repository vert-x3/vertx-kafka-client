package io.vertx.kafka.tests;

import io.vertx.core.Vertx;
import io.vertx.kafka.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class EventLoopThreadConsumerMockTest extends ConsumerMockTestBase {

  @Override
  <K, V> KafkaConsumer<K, V> createConsumer(Vertx vertx, Consumer<K, V> consumer) {
    return KafkaConsumer.create(vertx, consumer);
  }
}
