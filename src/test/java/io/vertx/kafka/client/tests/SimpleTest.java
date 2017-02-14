package io.vertx.kafka.client.tests;

import examples.VertxKafkaClientExamples;
import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.Pump;
import io.vertx.core.streams.ReadStream;
import io.vertx.kafka.client.common.Node;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.producer.impl.KafkaProducerRecordImpl;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
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

    // https://github.com/vert-x3/vertx-amqp-bridge/blob/master/src/test/java/io/vertx/amqpbridge/AmqpBridgeTest.java#L250
    // https://github.com/vert-x3/vertx-amqp-bridge/blob/master/src/test/java/io/vertx/amqpbridge/AmqpBridgeTest.java#L389

    // kafka-server-start /usr/local/etc/kafka/server.properties
    // kafka-topics --zookeeper localhost:2181 --list

/*
    File dataDir = Testing.Files.createTestingDirectory("cluster");
    KafkaCluster cluster = new KafkaCluster().usingDirectory(dataDir).withPorts(2181, 9092);

    System.out.println("starting");
    cluster.addBrokers(1);
    cluster.startup();
    System.out.println("started");

    System.out.println("cluster.brokerList() = " + cluster.brokerList());

    Map<String, Object> prodProps = new HashMap<>();
    prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    prodProps.put(ProducerConfig.ACKS_CONFIG, "all");
    prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    io.vertx.kafka.KafkaProducer<String, String> prod = io.vertx.kafka.KafkaProducer.create(Vertx.vertx(), prodProps);
    String topic = "the-topic";
    System.out.println("producing on " + topic);
    for (int i = 0;i < 100;i++) {
      prod.write(new ProducerRecord<>(topic, 0, "the-key", "the-value"));
    }
    prod.close();
    System.out.println("done");

    Map<String, Object> consProps = new HashMap<>();
    consProps.put("zookeeper.connect", "localhost:2181");
    consProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    consProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-producer");
    consProps.put("group.id", "consumer_2");
    consProps.put("enable.auto.commit", "false");
    consProps.put("auto.offset.reset", "earliest");
    consProps.put(ProducerConfig.ACKS_CONFIG, "all");
    consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    KafkaConsumer<String, String> cons = new KafkaConsumer<>(consProps);
    try {
      cons.subscribe(Collections.singleton("the-topic"));
      ConsumerRecords<String, String> records = cons.poll(5000);
      System.out.println(records.count());
    } finally {
      cons.close();
    }

    System.out.println("stopping");
    cluster.shutdown();
    System.out.println("stopped");
*/




/*
    Properties kafkaProperties = new Properties();
    Properties zkProperties = new Properties();
    kafkaProperties.load(new FileInputStream("/usr/local/etc/kafka/server.properties"));
    zkProperties.load(new FileInputStream("/usr/local/etc/zookeeper/zoo.cfg"));
    KafkaLocal kafka = new KafkaLocal(kafkaProperties, zkProperties);
*/

/*
    Map<String, Object> props = new HashMap<>();
    props.put("zookeeper.connect", "localhost:2181");
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-producer");
    props.put("group.id", "consumer_2");
    props.put("enable.auto.commit", "false");
    props.put("auto.offset.reset", "earliest");
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    KafkaConsumer<String, String> cons = new KafkaConsumer<>(props);
    try {
      cons.subscribe(Collections.singleton("topic-0f257314-056e-4cbd-8183-ec255383c7ea"));
      ConsumerRecords<String, String> records = cons.poll(5000);
      System.out.println(records.count());
    } finally {
      cons.close();
    }
*/




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

    Vertx vertx = Vertx.vertx();

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "my_client");


    io.vertx.kafka.client.consumer.KafkaConsumer<String, String> consumer =
      io.vertx.kafka.client.consumer.KafkaConsumer.create(vertx, props);

    Map<String, String> map = new HashMap<>();
    map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    map.put(ProducerConfig.ACKS_CONFIG, Integer.toString(1));

    io.vertx.kafka.client.producer.KafkaProducer<String, String> producer =
      io.vertx.kafka.client.producer.KafkaProducer.create(vertx, map, String.class, String.class);

    VertxKafkaClientExamples examples = new VertxKafkaClientExamples();

    //examples.example1(Vertx.vertx());

    //examples.example2(consumer);

    //examples.example3(consumer);

    //examples.example4(consumer);

    //examples.example5(consumer);

    //examples.example6(consumer);

    //examples.example7(consumer);

    //examples.example8(vertx, consumer);

    //examples.example9(producer);

    //examples.example10(producer);

    //examples.example11(producer);

    //examples.example12(consumer);

    /*KafkaConsumer c = new KafkaConsumer(props);

    c.subscribe(Collections.singleton("test"));

    while (true) {

      ConsumerRecords records = c.poll(1000);
      records.toString();
    }*/

    /*
    io.vertx.kafka.client.consumer.KafkaConsumer<String, String> c =
      io.vertx.kafka.client.consumer.KafkaConsumer.create(Vertx.vertx(), (new ConsumerOptions()).setWorkerThread(false), props);
    Set<String> topics = new HashSet<String>();
    topics.add("mytopic");
    c.subscribe(topics);
    c.handler(record -> {
      System.out.println(record.value());
    });
    */

    //Map<String, String> map = new HashMap<>();
    //map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    //map.put(ProducerConfig.ACKS_CONFIG, Integer.toString(1));
    //map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    //map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);


    /*
    io.vertx.kafka.client.producer.KafkaProducer<String, String> p =
      io.vertx.kafka.client.producer.KafkaProducer.create(Vertx.vertx(), props);

    p.partitionsFor("the_topic", done -> {

      if (done.succeeded()) {
        System.out.println(done.result());
      } else {
        System.out.println(done.cause());
      }

    });
    */

    //p.write(new KafkaProducerRecordImpl<>(new ProducerRecord<String, String>("the_topic", "Pippo")));

    /*io.vertx.kafka.client.producer.KafkaProducer<String, String> p =
      io.vertx.kafka.client.producer.KafkaProducer.create(Vertx.vertx(), props, String.class, String.class);

    p.write(new KafkaProducerRecordImpl<>(new ProducerRecord<String, String>("the_topic", "Pippo")));*/

    /*io.vertx.kafka.client.producer.KafkaProducer<String, String> p =
      io.vertx.kafka.client.producer.KafkaProducer.create(Vertx.vertx(), map);

    p.write(new KafkaProducerRecordImpl<>(new ProducerRecord<String, String>("the_topic", "Pippo")));*/

    /*
    io.vertx.kafka.client.producer.KafkaProducer<String, String> p =
      io.vertx.kafka.client.producer.KafkaProducer.create(Vertx.vertx(), map, String.class, String.class);

    p.write(new KafkaProducerRecordImpl<>(new ProducerRecord<String, String>("the_topic", "Pippo")));
    */


    /*
    io.vertx.kafka.client.producer.KafkaProducer<String, String> w =
      io.vertx.kafka.client.producer.KafkaProducer.create(Vertx.vertx(), props);

    w.write(new KafkaProducerRecordImpl<>(new ProducerRecord<String, String>("the_topic", "Pippo")), recordMetadata -> {
      System.out.println(recordMetadata.topic() + " " + recordMetadata.partition() + " " + recordMetadata.offset());
    });
    */

    /*
    KafkaWriteStream<String, String> c = KafkaWriteStream.create(Vertx.vertx(), props);

    c.drainHandler(v -> {
      System.out.println("drain");
    });
    c.exceptionHandler(e -> {
      System.out.println(e);
    });
    c.write(new ProducerRecord<>("the_topic", "Paolo"));
    */


    /*
    KafkaWriteStream<String, String> p = KafkaWriteStream.create(Vertx.vertx(), props);
    p.partitionsFor("the_topic", done -> {

      if (done.succeeded()) {
        System.out.println(done.result());
      } else {
        System.out.println(done.cause());
      }
    });
    */

    /*p.write(new ProducerRecord<String, String>("the_topic", "Paolo"), (recordMetadata, e) -> {

      System.out.println(recordMetadata.topic() + " " + recordMetadata.partition() + " " + recordMetadata.offset());
    });*/

    /*
    Node node = new Node()
      .setHasRack(false)
      .setHost("Host")
      .setId(1)
      .setIdString("idString")
      .setIsEmpty(true)
      .setPort(10)
      .setRack("Rack");

    List<Node> list = new ArrayList<>();
    list.add(node);
    list.add(node);

    PartitionInfo partition = new PartitionInfo()
      .setPartition(1)
      .setInSyncReplicas(list)
      .setLeader(node)
      .setReplicas(list)
      .setTopic("topic");

    JsonObject json = partition.toJson();

    PartitionInfo p = new PartitionInfo(json);
    */
    System.in.read();

  }
}
