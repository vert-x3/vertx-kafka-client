package io.vertx.kafka.client.tests;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;

public class EarliestTest extends AbstractVerticle {

  public static void main(String[] args) {

    Vertx vertx = Vertx.vertx();

    EarliestTest earliestTest = new EarliestTest();

    vertx.deployVerticle(earliestTest, res -> {
      if (res.succeeded()) {
        System.out.println("ok");
      } else {
        System.out.println("ko");
      }
    });


  }

  @Override
  public void start(Promise<Void> startFuture) throws Exception {

    Map<String, String> config = new HashMap<>();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    KafkaConsumer consumer = KafkaConsumer.create(vertx, config);
    consumer.handler(r -> {

      System.out.println(r);
    });
    consumer.subscribe("my-topic");

    startFuture.complete();
  }
}
