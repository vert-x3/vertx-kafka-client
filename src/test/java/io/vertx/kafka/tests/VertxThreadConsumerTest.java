package io.vertx.kafka.tests;

import io.vertx.core.Vertx;
import io.vertx.kafka.KafkaConsumer;
import io.vertx.kafka.impl.VertxThreadConsumer;

import java.util.Properties;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class VertxThreadConsumerTest extends ConsumerTestBase {

  @Override
  <K, V> KafkaConsumer<K, V> createConsumer(Vertx vertx, Properties config) {
    return VertxThreadConsumer.create(vertx, config);
  }
}
