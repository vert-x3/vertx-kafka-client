package io.vertx.kafka.client.tests;

import io.debezium.kafka.KafkaCluster;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class CleanupTest extends KafkaClusterTestBase {

  private Vertx vertx;

  @Before
  public void beforeTest() {
    vertx = Vertx.vertx();
  }

  @After
  public void afterTest(TestContext ctx) {
    vertx.close(ctx.asyncAssertSuccess());
  }

  private long countProducerThreads() {
    return Thread.getAllStackTraces().keySet()
      .stream()
      .filter(t -> t.getName().contains("kafka-producer-network-thread"))
      .count();
  }

  @Test
  public void testSharedProducer(TestContext ctx) throws Exception {
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    int num = 3;
    Async sentLatch = ctx.async(num);
    LinkedList<KafkaProducer<String, String>> producers = new LinkedList<>();
    for (int i = 0;i < num;i++) {
      KafkaProducer<String, String> producer = KafkaProducer.createShared(vertx, "the-name", config);
      producer.write(KafkaProducerRecord.create("the_topic", "the_value"), ctx.asyncAssertSuccess(v -> {
        sentLatch.countDown();
      }));
      producers.add(producer);
    }
    sentLatch.awaitSuccess(10000);
    Async async = ctx.async();
    kafkaCluster.useTo().consumeStrings("the_topic", num, 10, TimeUnit.SECONDS, () -> {
      close(ctx, producers, () -> {
        ctx.assertEquals(0L, countProducerThreads());
        async.complete();
      });
    });
  }

  private void close(TestContext ctx, LinkedList<KafkaProducer<String, String>> producers, Runnable doneHandler) {
    if (producers.size() > 0) {
      ctx.assertEquals(1L, countProducerThreads());
      KafkaProducer<String, String> producer = producers.removeFirst();
      producer.close(ctx.asyncAssertSuccess(v -> {
        close(ctx, producers, doneHandler);
      }));
    } else {
      doneHandler.run();
    }
  }

  public static class TheVerticle extends AbstractVerticle {
    @Override
    public void start(Future<Void> startFuture) throws Exception {
      Properties config = new Properties();
      config.putAll(context.config().getMap());
      KafkaProducer<String, String> producer = KafkaProducer.createShared(vertx, "the-name", config);
      producer.write(KafkaProducerRecord.create("the_topic", "the_value"), ar -> startFuture.handle(ar.map((Void) null)));
    }
  }

  @Test
  public void testSharedProducerCleanupInVerticle(TestContext ctx) throws Exception {
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    int num = 3;
    Async sentLatch = ctx.async(num);
    AtomicReference<String> deploymentID = new AtomicReference<>();
    vertx.deployVerticle(TheVerticle.class.getName(), new DeploymentOptions().setInstances(3).setConfig(new JsonObject((Map)config)),
      ctx.asyncAssertSuccess(id -> {
        deploymentID.set(id);
        sentLatch.complete();
      }));
    sentLatch.awaitSuccess(10000);
    Async async = ctx.async();
    kafkaCluster.useTo().consumeStrings("the_topic", num, 10, TimeUnit.SECONDS, () -> {
      vertx.undeploy(deploymentID.get(), ctx.asyncAssertSuccess(v -> {
        ctx.assertEquals(0L, countProducerThreads());
        async.complete();
      }));
    });
  }

  @Test
  public void testCleanupInProducer(TestContext ctx) throws Exception {
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
    String topicName = "testCleanupInConsumer";
    Properties config = kafkaCluster.useTo().getConsumerProperties("testCleanupInConsumer_consumer",
      "testCleanupInConsumer_consumer", OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    Async async = ctx.async(2);
    Async produceLatch = ctx.async();
    vertx.deployVerticle(new AbstractVerticle() {
      @Override
      public void start(Future<Void> fut) throws Exception {
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);
        consumer.handler(record -> {
          // Very rarely, this throws a AlreadyUndedeployed error
          vertx.undeploy(context.deploymentID(), ctx.asyncAssertSuccess(ar -> {
            try {
              // Race condition? Without a sleep, test fails sometimes
              Thread.sleep(10);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            Thread.getAllStackTraces().forEach((t, s) -> {
              if (t.getName().contains("vert.x-kafka-consumer-thread")) {
                ctx.fail("Was expecting the consumer to be closed");
              }
            });
            async.countDown();
          }));
        });
        consumer.assign(new TopicPartition(topicName, 0), fut);
      }
    }, ctx.asyncAssertSuccess(v ->  produceLatch.complete()
    ));
    produceLatch.awaitSuccess(10000);
    kafkaCluster.useTo().produce("testCleanupInConsumer_producer", 100,
      new StringSerializer(), new StringSerializer(), async::countDown,
      () -> new ProducerRecord<>(topicName, "the_value"));
  }

  @Test
  // Regression test for ISS-73: undeployment of a verticle with unassigned consumer fails
  public void testUndeployUnassignedConsumer(TestContext ctx) throws Exception {
    Properties config = kafkaCluster.useTo().getConsumerProperties("testUndeployUnassignedConsumer_consumer",
      "testUndeployUnassignedConsumer_consumer", OffsetResetStrategy.EARLIEST);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    Async async = ctx.async(1);
    vertx.deployVerticle(new AbstractVerticle() {
      @Override
      public void start() throws Exception {
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);
        vertx.setTimer(20, record -> {
          // Very rarely, this throws a AlreadyUndedeployed error
          vertx.undeploy(context.deploymentID(), ctx.asyncAssertSuccess(ar -> {
            async.complete();
          }));
        });
      }
    }, ctx.asyncAssertSuccess());
  }
}
