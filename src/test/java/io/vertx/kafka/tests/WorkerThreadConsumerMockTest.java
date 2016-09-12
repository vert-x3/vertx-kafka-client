package io.vertx.kafka.tests;

import io.vertx.core.Vertx;
import io.vertx.kafka.consumer.ConsumerOptions;
import io.vertx.kafka.consumer.KafkaReadStream;
import org.apache.kafka.clients.consumer.Consumer;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class WorkerThreadConsumerMockTest extends ConsumerMockTestBase {

  @Override
  <K, V> KafkaReadStream<K, V> createConsumer(Vertx vertx, Consumer<K, V> consumer) {
    return KafkaReadStream.create(vertx, new ConsumerOptions().setWorkerThread(true), consumer);
  }
}
