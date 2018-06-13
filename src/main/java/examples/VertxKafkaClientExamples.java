/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package examples;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.docgen.Source;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.consumer.OffsetAndTimestamp;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@Source
public class VertxKafkaClientExamples {

  /**
   * Example about Kafka consumer and producer creation
   * @param vertx
   */
  public void exampleCreateConsumerJava(Vertx vertx) {

    // creating the consumer using properties config
    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    // use consumer for interacting with Apache Kafka
    KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);
  }

  /**
   * Example about Kafka consumer and producer creation
   * @param vertx
   */
  public void exampleCreateConsumer(Vertx vertx) {

    // creating the consumer using map config
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("group.id", "my_group");
    config.put("auto.offset.reset", "earliest");
    config.put("enable.auto.commit", "false");

    // use consumer for interacting with Apache Kafka
    KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);
  }

  public void createProducer(Vertx vertx) {

    // creating the producer using map and class types for key and value serializers/deserializers
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("acks", "1");

    // use producer for interacting with Apache Kafka
    KafkaProducer<String, String> producer = KafkaProducer.create(vertx, config);
  }

  /**
   * Example about Kafka consumer and producer creation
   * @param vertx
   */
  public void createProducerJava(Vertx vertx) {

    // creating the producer using map and class types for key and value serializers/deserializers
    Properties config = new Properties();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ProducerConfig.ACKS_CONFIG, "1");

    // use producer for interacting with Apache Kafka
    KafkaProducer<String, String> producer = KafkaProducer.create(vertx, config, String.class, String.class);
  }

  public void exampleSubscribe(KafkaConsumer<String, String> consumer) {

    // register the handler for incoming messages
    consumer.handler(record -> {
      System.out.println("Processing key=" + record.key() + ",value=" + record.value() +
        ",partition=" + record.partition() + ",offset=" + record.offset());
    });

    // subscribe to several topics
    Set<String> topics = new HashSet<>();
    topics.add("topic1");
    topics.add("topic2");
    topics.add("topic3");
    consumer.subscribe(topics);

    // or just subscribe to a single topic
    consumer.subscribe("a-single-topic");
  }

  public void exampleSubscribeWithResult(KafkaConsumer<String, String> consumer) {

    // register the handler for incoming messages
    consumer.handler(record -> {
      System.out.println("Processing key=" + record.key() + ",value=" + record.value() +
        ",partition=" + record.partition() + ",offset=" + record.offset());
    });

    // subscribe to several topics
    Set<String> topics = new HashSet<>();
    topics.add("topic1");
    topics.add("topic2");
    topics.add("topic3");
    consumer.subscribe(topics, ar -> {
      if (ar.succeeded()) {
        System.out.println("subscribed");
      } else {
        System.out.println("Could not subscribe " + ar.cause().getMessage());
      }
    });

    // or just subscribe to a single topic
    consumer.subscribe("a-single-topic", ar -> {
      if (ar.succeeded()) {
        System.out.println("subscribed");
      } else {
        System.out.println("Could not subscribe " + ar.cause().getMessage());
      }
    });
  }

  /**
   * Example about how Kafka consumer receives messages
   * from a topic being part of a consumer group
   * @param consumer
   */
  public void exampleConsumerPartitionsNotifs(KafkaConsumer<String, String> consumer) {

    // register the handler for incoming messages
    consumer.handler(record -> {
      System.out.println("Processing key=" + record.key() + ",value=" + record.value() +
        ",partition=" + record.partition() + ",offset=" + record.offset());
    });

    // registering handlers for assigned and revoked partitions
    consumer.partitionsAssignedHandler(topicPartitions -> {

      System.out.println("Partitions assigned");
      for (TopicPartition topicPartition : topicPartitions) {
        System.out.println(topicPartition.getTopic() + " " + topicPartition.getPartition());
      }
    });

    consumer.partitionsRevokedHandler(topicPartitions -> {

      System.out.println("Partitions revoked");
      for (TopicPartition topicPartition : topicPartitions) {
        System.out.println(topicPartition.getTopic() + " " + topicPartition.getPartition());
      }
    });

    // subscribes to the topic
    consumer.subscribe("test", ar -> {

      if (ar.succeeded()) {
        System.out.println("Consumer subscribed");
      }
    });
  }

  public void exampleUnsubscribe(KafkaConsumer<String, String> consumer) {

    // consumer is already member of a consumer group

    // unsubscribing request
    consumer.unsubscribe();
  }

  /**
   * Example about how Kafka consumer can leave
   * a previous joined consumer group
   * @param consumer
   */
  public void exampleUnsubscribeWithCallback(KafkaConsumer<String, String> consumer) {

    // consumer is already member of a consumer group

    // unsubscribing request
    consumer.unsubscribe(ar -> {

      if (ar.succeeded()) {
        System.out.println("Consumer unsubscribed");
      }
    });
  }

  /**
   * Example about how Kafka consumer receives messages
   * from a topic requesting a specific partition for that
   * @param consumer
   */
  public void exampleConsumerAssignPartition(KafkaConsumer<String, String> consumer) {

    // register the handler for incoming messages
    consumer.handler(record -> {
      System.out.println("key=" + record.key() + ",value=" + record.value() +
        ",partition=" + record.partition() + ",offset=" + record.offset());
    });

    //
    Set<TopicPartition> topicPartitions = new HashSet<>();
    topicPartitions.add(new TopicPartition()
      .setTopic("test")
      .setPartition(0));

    // requesting to be assigned the specific partition
    consumer.assign(topicPartitions, done -> {

      if (done.succeeded()) {
        System.out.println("Partition assigned");

        // requesting the assigned partitions
        consumer.assignment(done1 -> {

          if (done1.succeeded()) {

            for (TopicPartition topicPartition : done1.result()) {
              System.out.println(topicPartition.getTopic() + " " + topicPartition.getPartition());
            }
          }
        });
      }
    });
  }

  /**
   * Example about how it's possible to get information
   * on partitions for a specified topic (valid for both
   * Kafka consumer and producer instances)
   * @param consumer
   */
  public void exampleConsumerPartitionsFor(KafkaConsumer<String, String> consumer) {

    // asking partitions information about specific topic
    consumer.partitionsFor("test", ar -> {

      if (ar.succeeded()) {

        for (PartitionInfo partitionInfo : ar.result()) {
          System.out.println(partitionInfo);
        }
      }
    });
  }

  public void exampleConsumerListTopics(KafkaConsumer<String, String> consumer) {

    // asking information about available topics and related partitions
    consumer.listTopics(ar -> {

      if (ar.succeeded()) {

        Map<String, List<PartitionInfo>> map = ar.result();
        map.forEach((topic, partitions) -> {
          System.out.println("topic = " + topic);
          System.out.println("partitions = " + map.get(topic));
        });
      }
    });
  }

  /**
   * Example about how it's possible to get messages from Kafka
   * doing poll at application level instead of using the internal
   * one in the client
   *
   * @param vertx
   * @param consumer
   */
  public void exampleConsumerWithPoll(Vertx vertx, KafkaConsumer<String, String> consumer) {

    // subscribes to the topic
    consumer.subscribe("test", ar -> {

      if (ar.succeeded()) {
        System.out.println("Consumer subscribed");

        vertx.setPeriodic(1000, timerId -> {

          consumer.poll(100, ar1 -> {

            if (ar1.succeeded()) {

              KafkaConsumerRecords<String, String> records = ar1.result();
              for (int i = 0; i < records.size(); i++) {
                KafkaConsumerRecord<String, String> record = records.recordAt(i);
                System.out.println("key=" + record.key() + ",value=" + record.value() +
                  ",partition=" + record.partition() + ",offset=" + record.offset());
              }
            }
          });

        });
      }
    });
  }

  public void exampleProducerPartitionsFor(KafkaProducer<String, String> producer) {

    // asking partitions information about specific topic
    producer.partitionsFor("test", ar -> {

      if (ar.succeeded()) {

        for (PartitionInfo partitionInfo : ar.result()) {
          System.out.println(partitionInfo);
        }
      }
    });
  }

  /**
   * Example about how Kafka consumer can handle manual commit
   * of the current offset for a topic partition
   * @param consumer
   */
  public void exampleConsumerManualOffsetCommit(KafkaConsumer<String, String> consumer) {

    // consumer is processing read messages

    // committing offset of the last read message
    consumer.commit(ar -> {

      if (ar.succeeded()) {
        System.out.println("Last read message offset committed");
      }
    });
  }

  public void exampleSeek(KafkaConsumer<String, String> consumer) {

    TopicPartition topicPartition = new TopicPartition()
      .setTopic("test")
      .setPartition(0);

    // seek to a specific offset
    consumer.seek(topicPartition, 10, done -> {

      if (done.succeeded()) {
        System.out.println("Seeking done");
      }
    });

  }

  public void exampleSeekToBeginning(KafkaConsumer<String, String> consumer) {

    TopicPartition topicPartition = new TopicPartition()
      .setTopic("test")
      .setPartition(0);

    // seek to the beginning of the partition
    consumer.seekToBeginning(Collections.singleton(topicPartition), done -> {

      if (done.succeeded()) {
        System.out.println("Seeking done");
      }
    });
  }

  /**
   * Example about how Kafka consumer can seek in the partition
   * changing the offset from which starting to read messages
   * @param consumer
   */
  public void exampleSeekToEnd(KafkaConsumer<String, String> consumer) {

    TopicPartition topicPartition = new TopicPartition()
      .setTopic("test")
      .setPartition(0);

    // seek to the end of the partition
    consumer.seekToEnd(Collections.singleton(topicPartition), done -> {

      if (done.succeeded()) {
        System.out.println("Seeking done");
      }
    });
  }


  /**
   * Example to demonstrate how one can use the new beginningOffsets API (introduced with Kafka
   * 0.10.1.1) to look up the first offset for a given partition.
   * @param consumer Consumer to be used
   */
  public void exampleConsumerBeginningOffsets(KafkaConsumer<String, String> consumer) {
    Set<TopicPartition> topicPartitions = new HashSet<>();
    TopicPartition topicPartition = new TopicPartition().setTopic("test").setPartition(0);
    topicPartitions.add(topicPartition);

    consumer.beginningOffsets(topicPartitions, done -> {
      if(done.succeeded()) {
        Map<TopicPartition, Long> results = done.result();
        results.forEach((topic, beginningOffset) ->
          System.out.println("Beginning offset for topic="+topic.getTopic()+", partition="+
            topic.getPartition()+", beginningOffset="+beginningOffset));
      }
    });

    // Convenience method for single-partition lookup
    consumer.beginningOffsets(topicPartition, done -> {
      if(done.succeeded()) {
        Long beginningOffset = done.result();
          System.out.println("Beginning offset for topic="+topicPartition.getTopic()+", partition="+
            topicPartition.getPartition()+", beginningOffset="+beginningOffset);
      }
    });

  }

  /**
   * Example to demonstrate how one can use the new endOffsets API (introduced with Kafka
   * 0.10.1.1) to look up the last offset for a given partition.
   * @param consumer Consumer to be used
   */
  public void exampleConsumerEndOffsets(KafkaConsumer<String, String> consumer) {
    Set<TopicPartition> topicPartitions = new HashSet<>();
    TopicPartition topicPartition = new TopicPartition().setTopic("test").setPartition(0);
    topicPartitions.add(topicPartition);

    consumer.endOffsets(topicPartitions, done -> {
      if(done.succeeded()) {
        Map<TopicPartition, Long> results = done.result();
        results.forEach((topic, endOffset) ->
          System.out.println("End offset for topic="+topic.getTopic()+", partition="+
            topic.getPartition()+", endOffset="+endOffset));
      }
    });

    // Convenience method for single-partition lookup
    consumer.endOffsets(topicPartition, done -> {
      if(done.succeeded()) {
        Long endOffset = done.result();
          System.out.println("End offset for topic="+topicPartition.getTopic()+", partition="+
            topicPartition.getPartition()+", endOffset="+endOffset);
      }
    });
  }

  /**
   * Example to demonstrate how one can use the new offsetsForTimes API (introduced with Kafka
   * 0.10.1.1) to look up an offset by timestamp, i.e. search parameter is an epoch timestamp and
   * the call returns the lowest offset with ingestion timestamp >= given timestamp.
   * @param consumer Consumer to be used
   */
  public void exampleConsumerOffsetsForTimes(KafkaConsumer<String, String> consumer) {
    Map<TopicPartition, Long> topicPartitionsWithTimestamps = new HashMap<>();
    TopicPartition topicPartition = new TopicPartition().setTopic("test").setPartition(0);

    // We are interested in the offset for data ingested 60 seconds ago
    long timestamp = (System.currentTimeMillis() - 60000);

    topicPartitionsWithTimestamps.put(topicPartition, timestamp);
    consumer.offsetsForTimes(topicPartitionsWithTimestamps, done -> {
      if(done.succeeded()) {
        Map<TopicPartition, OffsetAndTimestamp> results = done.result();
        results.forEach((topic, offset) ->
          System.out.println("Offset for topic="+topic.getTopic()+
            ", partition="+topic.getPartition()+"\n"+
            ", timestamp="+timestamp+", offset="+offset.getOffset()+
            ", offsetTimestamp="+offset.getTimestamp()));

      }
    });

    // Convenience method for single-partition lookup
    consumer.offsetsForTimes(topicPartition, timestamp, done -> {
      if(done.succeeded()) {
        OffsetAndTimestamp offsetAndTimestamp = done.result();
          System.out.println("Offset for topic="+topicPartition.getTopic()+
            ", partition="+topicPartition.getPartition()+"\n"+
            ", timestamp="+timestamp+", offset="+offsetAndTimestamp.getOffset()+
            ", offsetTimestamp="+offsetAndTimestamp.getTimestamp());

      }
    });
  }

  /**
   * Example about how Kafka consumer can pause reading from a topic partition
   * and then resume read operation for continuing to get messages
   * @param vertx
   * @param consumer
   */
  public void exampleConsumerFlowControl(Vertx vertx, KafkaConsumer<String, String> consumer) {

    TopicPartition topicPartition = new TopicPartition()
      .setTopic("test")
      .setPartition(0);

    // registering the handler for incoming messages
    consumer.handler(record -> {
      System.out.println("key=" + record.key() + ",value=" + record.value() +
        ",partition=" + record.partition() + ",offset=" + record.offset());

      // i.e. pause/resume on partition 0, after reading message up to offset 5
      if ((record.partition() == 0) && (record.offset() == 5)) {

        // pause the read operations
        consumer.pause(topicPartition, ar -> {

          if (ar.succeeded()) {

            System.out.println("Paused");

            // resume read operation after a specific time
            vertx.setTimer(5000, timeId -> {

              // resumi read operations
              consumer.resume(topicPartition);
            });
          }
        });
      }
    });
  }


  public void exampleConsumerClose(KafkaConsumer<String, String> consumer) {
    consumer.close(res -> {
      if (res.succeeded()) {
        System.out.println("Consumer is now closed");
      } else {
        System.out.println("close failed");
      }
    });
  }

  public void exampleProducerWrite(KafkaProducer<String, String> producer) {

    for (int i = 0; i < 5; i++) {

      // only topic and message value are specified, round robin on destination partitions
      KafkaProducerRecord<String, String> record =
        KafkaProducerRecord.create("test", "message_" + i);

      producer.write(record);
    }
  }

  /**
   * Example about how Kafka producer sends message to topic
   * partitions in a round robin fashion
   * @param producer
   */
  public void exampleProducerWriteWithAck(KafkaProducer<String, String> producer) {

    for (int i = 0; i < 5; i++) {

      // only topic and message value are specified, round robin on destination partitions
      KafkaProducerRecord<String, String> record =
        KafkaProducerRecord.create("test", "message_" + i);

      producer.write(record, done -> {

        if (done.succeeded()) {

          RecordMetadata recordMetadata = done.result();
          System.out.println("Message " + record.value() + " written on topic=" + recordMetadata.getTopic() +
            ", partition=" + recordMetadata.getPartition() +
            ", offset=" + recordMetadata.getOffset());
        }

      });
    }

  }

  /**
   * Example about how Kafka producer sends messages to a
   * specified topic partition
   * @param producer
   */
  public void exampleProducerWriteWithSpecificPartition(KafkaProducer<String, String> producer) {

    for (int i = 0; i < 10; i++) {

      // a destination partition is specified
      KafkaProducerRecord<String, String> record =
        KafkaProducerRecord.create("test", null, "message_" + i, 0);

      producer.write(record);
    }
  }

  /**
   * Example about how Kafka producer sends messages
   * specifying a key hashed internally for defining
   * the destination partition
   * @param producer
   */
  public void exampleProducerWriteWithSpecificKey(KafkaProducer<String, String> producer) {

    for (int i = 0; i < 10; i++) {

      // i.e. defining different keys for odd and even messages
      int key = i % 2;

      // a key is specified, all messages with same key will be sent to the same partition
      KafkaProducerRecord<String, String> record =
        KafkaProducerRecord.create("test", String.valueOf(key), "message_" + i);

      producer.write(record);
    }
  }

  public void exampleSharedProducer(Vertx vertx, Map<String, String> config) {

    // Create a shared producer identified by 'the-producer'
    KafkaProducer<String, String> producer1 = KafkaProducer.createShared(vertx, "the-producer", config);

    // Sometimes later you can close it
    producer1.close();
  }

  public void exampleProducerClose(KafkaProducer<String, String> producer) {
    producer.close(res -> {
      if (res.succeeded()) {
        System.out.println("Producer is now closed");
      } else {
        System.out.println("close failed");
      }
    });
  }

  /**
   * Example about how Kafka client (producer or consumer)
   * can handle errors and exception during communication
   * with the Kafka cluster
   * @param consumer
   */
  public void exampleErrorHandling(KafkaConsumer<String, String> consumer) {

    // setting handler for errors
    consumer.exceptionHandler(e -> {
      System.out.println("Error = " + e.getMessage());
    });
  }

  public void exampleUsingVertxDeserializers() {

    // Creating a consumer able to deserialize to buffers
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.deserializer", "io.vertx.kafka.client.serialization.BufferDeserializer");
    config.put("value.deserializer", "io.vertx.kafka.client.serialization.BufferDeserializer");
    config.put("group.id", "my_group");
    config.put("auto.offset.reset", "earliest");
    config.put("enable.auto.commit", "false");

    // Creating a consumer able to deserialize to json object
    config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.deserializer", "io.vertx.kafka.client.serialization.JsonObjectDeserializer");
    config.put("value.deserializer", "io.vertx.kafka.client.serialization.JsonObjectDeserializer");
    config.put("group.id", "my_group");
    config.put("auto.offset.reset", "earliest");
    config.put("enable.auto.commit", "false");

    // Creating a consumer able to deserialize to json array
    config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.deserializer", "io.vertx.kafka.client.serialization.JsonArrayDeserializer");
    config.put("value.deserializer", "io.vertx.kafka.client.serialization.JsonArrayDeserializer");
    config.put("group.id", "my_group");
    config.put("auto.offset.reset", "earliest");
    config.put("enable.auto.commit", "false");
  }

  public void exampleUsingVertxSerializers() {

    // Creating a producer able to serialize to buffers
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.serializer", "io.vertx.kafka.client.serialization.BufferSerializer");
    config.put("value.serializer", "io.vertx.kafka.client.serialization.BufferSerializer");
    config.put("acks", "1");

    // Creating a producer able to serialize to json object
    config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.serializer", "io.vertx.kafka.client.serialization.JsonObjectSerializer");
    config.put("value.serializer", "io.vertx.kafka.client.serialization.JsonObjectSerializer");
    config.put("acks", "1");

    // Creating a producer able to serialize to json array
    config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.serializer", "io.vertx.kafka.client.serialization.JsonArraySerializer");
    config.put("value.serializer", "io.vertx.kafka.client.serialization.JsonArraySerializer");
    config.put("acks", "1");
  }

  public void exampleUsingVertxDeserializers2(Vertx vertx) {

    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("group.id", "my_group");
    config.put("auto.offset.reset", "earliest");
    config.put("enable.auto.commit", "false");

    // Creating a consumer able to deserialize buffers
    KafkaConsumer<Buffer, Buffer> bufferConsumer = KafkaConsumer.create(vertx, config, Buffer.class, Buffer.class);

    // Creating a consumer able to deserialize json objects
    KafkaConsumer<JsonObject, JsonObject> jsonObjectConsumer = KafkaConsumer.create(vertx, config, JsonObject.class, JsonObject.class);

    // Creating a consumer able to deserialize json arrays
    KafkaConsumer<JsonArray, JsonArray> jsonArrayConsumer = KafkaConsumer.create(vertx, config, JsonArray.class, JsonArray.class);
  }

  public void exampleUsingVertxSerializers2(Vertx vertx) {

    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("acks", "1");

    // Creating a producer able to serialize to buffers
    KafkaProducer<Buffer, Buffer> bufferProducer = KafkaProducer.create(vertx, config, Buffer.class, Buffer.class);

    // Creating a producer able to serialize to json objects
    KafkaProducer<JsonObject, JsonObject> jsonObjectProducer = KafkaProducer.create(vertx, config, JsonObject.class, JsonObject.class);

    // Creating a producer able to serialize to json arrays
    KafkaProducer<JsonArray, JsonArray> jsonArrayProducer = KafkaProducer.create(vertx, config, JsonArray.class, JsonArray.class);
  }
}
