package io.vertx.kafka.client.tests;

import io.debezium.kafka.KafkaCluster;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetServer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class ProducerTest extends KafkaClusterTestBase {

  private Vertx vertx;
  private KafkaWriteStream<String, String> producer;

  @Before
  public void beforeTest() {
    vertx = Vertx.vertx();
  }

  @After
  public void afterTest(TestContext ctx) {
    close(ctx, producer);
    vertx.close(ctx.asyncAssertSuccess());
    super.afterTest(ctx);
  }

  @Test
  public void testProduce(TestContext ctx) throws Exception {
    KafkaCluster kafkaCluster = kafkaCluster().addBrokers(1).startup();
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producer = producer(fut -> KafkaWriteStream.create(Vertx.vertx(), config, fut.completer()));
    producer.exceptionHandler(ctx::fail);
    int numMessages = 100000;
    for (int i = 0;i < numMessages;i++) {
      producer.write(new ProducerRecord<>("the_topic", 0, "key-" + i, "value-" + i));
    }
    Async done = ctx.async();
    AtomicInteger seq = new AtomicInteger();
    kafkaCluster.useTo().consumeStrings("the_topic", numMessages, 10, TimeUnit.SECONDS, done::complete, (key, value) -> {
      int count = seq.getAndIncrement();
      ctx.assertEquals("key-" + count, key);
      ctx.assertEquals("value-" + count, value);
      return true;
    });
  }

  @Test
  public void testBlockingBroker(TestContext ctx) throws Exception {
    Async serverAsync = ctx.async();
    NetServer server = vertx.createNetServer().connectHandler(so -> {
    }).listen(9092, ctx.asyncAssertSuccess(v -> serverAsync.complete()));
    serverAsync.awaitSuccess(10000);
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.setProperty(ProducerConfig.ACKS_CONFIG, Integer.toString(1));
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    KafkaWriteStream.create(Vertx.vertx(), props, ctx.asyncAssertFailure());
  }

  @Test
  public void testBrokerConnectionError(TestContext ctx) throws Exception {
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.setProperty(ProducerConfig.ACKS_CONFIG, Integer.toString(1));
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    KafkaWriteStream.create(Vertx.vertx(), props, ctx.asyncAssertFailure());
  }
}
