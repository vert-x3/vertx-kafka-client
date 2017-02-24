package io.vertx.kafka.client.tests;

import io.debezium.kafka.KafkaCluster;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.producer.impl.KafkaWriteStreamImpl;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class VerticlesTest extends KafkaClusterTestBase {

  private Vertx vertx;

  @Before
  public void beforeTest() {
    vertx = Vertx.vertx();
  }

  @After
  public void afterTest(TestContext ctx) {
    vertx.close(ctx.asyncAssertSuccess());
    super.afterTest(ctx);
  }

  @Test
  public void testCleanupInProducer(TestContext ctx) throws Exception {
    KafkaCluster kafkaCluster = kafkaCluster().addBrokers(1).startup();
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    AtomicReference<KafkaProducer<String, String>> producerRef = new AtomicReference<>();
    AtomicReference<String> deploymentRef = new AtomicReference<>();
    vertx.deployVerticle(new AbstractVerticle() {
      @Override
      public void start() throws Exception {
        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, config);
        producerRef.set(producer);
        producer.write(KafkaProducerRecord.create("the_topic", "the_value"), ctx.asyncAssertSuccess());
      }
    }, ctx.asyncAssertSuccess(deploymentRef::set));

    Async async = ctx.async();

    kafkaCluster.useTo().consumeStrings("the_topic", 1, 10, TimeUnit.SECONDS, () -> {
      vertx.undeploy(deploymentRef.get(), ctx.asyncAssertSuccess(v -> {
        Thread.getAllStackTraces().forEach((t, s) -> {
          if (t.getName().contains("kafka-producer-network-thread")) {
            ctx.fail("Was expecting the producer to be closed");
          }
        });
        async.complete();
      }));
    });
  }

  @Test
  public void testCleanupInConsumer(TestContext ctx) throws Exception {
    KafkaCluster kafkaCluster = kafkaCluster().addBrokers(1).startup();
    Properties config = kafkaCluster.useTo().getConsumerProperties("the_consumer", "the_consumer", OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    Async async = ctx.async();
    Async produceLatch = ctx.async();
    vertx.deployVerticle(new AbstractVerticle() {
      @Override
      public void start(Future<Void> fut) throws Exception {
        KafkaConsumer<String, String> producer = KafkaConsumer.create(vertx, config);
        producer.handler(record -> {
          vertx.undeploy(context.deploymentID(), ar -> {
            Thread.getAllStackTraces().forEach((t, s) -> {
              if (t.getName().contains("vert.x-kafka-consumer-thread")) {
                ctx.fail("Was expecting the consumer to be closed");
              }
            });
            async.complete();
          });
        });
        producer.subscribe("the_topic", fut);
      }
    }, ctx.asyncAssertSuccess(v -> produceLatch.complete()));
    produceLatch.awaitSuccess(10000);
    kafkaCluster.useTo().produce("the_producer", 1, new StringSerializer(), new StringSerializer(), () -> {
    }, () -> new ProducerRecord<>("the_topic", "the_value"));
  }
}
