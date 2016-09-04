package io.vertx.kafka.tests;

import io.vertx.core.Vertx;
import io.vertx.core.streams.Pump;
import io.vertx.core.streams.ReadStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class SimpleTest {

/*
  private void foo() {

    ReadStream<String> abc = null;
    io.vertx.kafka.KafkaProducer prod = null;
    Pump.pump(abc.map(s -> new KafkaProducerRecord().setKey("abc").setValue("" + s)), prod);

  }
*/

  public static void main(String[] args) throws Exception {

    Map<String, Object> props = new HashMap<>();
//    props.put("zookeeper.connect", "localhost:2181");
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
    props.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class);

//    org.apache.kafka.clients.producer.Producer<String, String> prod = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
//    for (int i = 0;i < 78;i++) {
//      System.out.println("sending " + i);
//      prod.send(new ProducerRecord<>("testtopic", 0, 0L, "key-" + i, "value-" + i));
//    }

/*
    io.vertx.kafka.KafkaProducer prod = io.vertx.kafka.KafkaProducer.create(
        Vertx.vertx(),
        Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"),
        KafkaType.STRING,
        KafkaType.STRING
    );
    for (int i = 0;i < 78;i++) {
      System.out.println("sending " + i);
      prod.write(new KafkaProducerRecord().setTopic("testtopic").setPartition(0).setTimestamp(0L).setKey("key-" + i).setValue("value-" + i));
    }
*/



    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-producer");
    props.put("group.id", "consumer_2");
    props.put("enable.auto.commit", "false");
    props.put("auto.offset.reset", "earliest");
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

/*
    Vertx vertx = Vertx.vertx();
    io.vertx.kafka.KafkaProducer producer = io.vertx.kafka.KafkaProducer.create(vertx, props, KafkaType.STRING, KafkaType.STRING);
    for (int i = 0;i < 100;i++) {
      producer.write(new KafkaProducerRecord().setTopic("testtopic").setPartition(0).setTimestamp(0L).setKey("" + i).setValue("hello-" + i));
    }
*/



/*
    KafkaConsumer consumer = KafkaConsumer.create(vertx, props, KafkaType.STRING, KafkaType.STRING);
    consumer.subscribe(Collections.singleton("testtopic"));
    consumer.handler(rec -> {
      System.out.println("GOT RECORD");
    });
*/


/*
    Vertx vertx = Vertx.vertx();
    AtomicInteger count = new AtomicInteger();

    KafkaProducer<Integer, byte[]> producer = new KafkaProducer<>(props);

    int size = 1024;
    Random r = new Random();
    byte[] bytes = new byte[size];

    vertx.setPeriodic(1000, id -> {
      System.out.println(count);
    });

    int len = 1000000;
    while (len-- > 0) {
      r.nextBytes(bytes);
      producer.send(new ProducerRecord<>("test", 0, 0L, 3, bytes), (a, b) -> {
        count.decrementAndGet();
      });
      count.incrementAndGet();
      Thread.yield();
    }
*/









  }
}
