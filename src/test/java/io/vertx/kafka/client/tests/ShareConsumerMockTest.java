package io.vertx.kafka.client.tests;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.ShareConsumer;

public class ShareConsumerMockTest extends ShareConsumerMockTestBase {

  @Override
  <K, V> KafkaShareConsumer<K, V> createShareConsumer(Vertx vertx, ShareConsumer<K, V> consumer) {
    return KafkaShareConsumer.create(vertx, consumer);
  }

}
