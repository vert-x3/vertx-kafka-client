package io.vertx.kafka.client.tests;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaShareConsumer;

import java.util.Properties;

/**
 * Integration tests for {@link KafkaShareConsumer} against a real Kafka cluster.
 */
public class ShareConsumerTest extends ShareConsumerTestBase {

  @Override
  protected KafkaShareConsumer<String, String> createShareConsumer(Vertx vertx, Properties config) {
    return KafkaShareConsumer.create(vertx, config);
  }
}
