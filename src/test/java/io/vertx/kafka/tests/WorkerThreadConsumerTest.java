package io.vertx.kafka.tests;

import io.vertx.core.Vertx;
import io.vertx.kafka.consumer.ConsumerOptions;
import io.vertx.kafka.consumer.KafkaReadStream;

import java.util.Properties;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class WorkerThreadConsumerTest extends ConsumerTestBase {

  @Override
  <K, V> KafkaReadStream<K, V> createConsumer(Vertx vertx, Properties config) {
    return KafkaReadStream.create(vertx, new ConsumerOptions().setWorkerThread(true), config);
  }
}
