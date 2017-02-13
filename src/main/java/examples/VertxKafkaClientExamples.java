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
import io.vertx.docgen.Source;
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
  public void example1(Vertx vertx) {

    // creating the consumer using properties
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, props);

    // creating the producer using map and class types for key and value serializers/deserializers
    Map<String, String> map = new HashMap<>();
    map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    map.put(ProducerConfig.ACKS_CONFIG, Integer.toString(1));

    KafkaProducer<String, String> producer = KafkaProducer.create(vertx, map, String.class, String.class);

    // using consumer and producer for interacting with Apache Kafka
  }

  /**
   * Example about how Kafka consumer receives messages
   * from a topic being part of a consumer group
   * @param consumer
   */
  public void example2(KafkaConsumer<String, String> consumer) {

    // registering the handler for incoming messages
    consumer.handler(record -> {
      System.out.println("key=" + record.key() + ",value=" + record.value() +
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

    // subscribing to the topic
    consumer.subscribe("test", done -> {

      if (done.succeeded()) {
        System.out.println("Consumer subscribed");
      }
    });
  }

  /**
   * Example about how Kafka consumer can leave
   * a previous joined consumer group
   * @param consumer
   */
  public void example3(KafkaConsumer<String, String> consumer) {

    // consumer is already member of a consumer group

    // unsubscribing request
    consumer.unsubscribe(done -> {

      if (done.succeeded()) {
        System.out.println("Consumer unsubscribed");
      }
    });
  }

  /**
   * Example about how Kafka consumer receives messages
   * from a topic requesting a specific partition for that
   * @param consumer
   */
  public void example4(KafkaConsumer<String, String> consumer) {

    Set<TopicPartition> topicPartitions = new HashSet<>();
    topicPartitions.add(new TopicPartition("test", 0)
      .setTopic("test")
      .setPartition(0));

    // registering the handler for incoming messages
    consumer.handler(record -> {
      System.out.println("key=" + record.key() + ",value=" + record.value() +
        ",partition=" + record.partition() + ",offset=" + record.offset());
    });

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
  public void example5(KafkaConsumer<String, String> consumer) {

    // asking information about available topics and related partitions
    consumer.listTopics(done -> {

      if (done.succeeded()) {

        Map<String, List<PartitionInfo>> map = done.result();
        map.forEach((topic, partitions) -> {
          System.out.println("topic = " + topic);
          System.out.println("partitions = " + map.get(topic));
        });
      }
    });

    // asking partitions information about specific topic
    consumer.partitionsFor("test", done -> {

      if (done.succeeded()) {

        for (PartitionInfo partitionInfo : done.result()) {
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
  public void example6(KafkaConsumer<String, String> consumer) {

    // consumer is processing read messages

    // committing offset of the last read message
    consumer.commit(done -> {

      if (done.succeeded()) {
        System.out.println("Last read message offset committed");
      }
    });
  }

  /**
   * Example about how Kafka consumer can seek in the partition
   * changing the offset from which starting to read messages
   * @param consumer
   */
  public void example7(KafkaConsumer<String, String> consumer) {

    TopicPartition topicPartition = new TopicPartition()
      .setTopic("test")
      .setPartition(0);

    // seeking to a specific offset
    consumer.seek(topicPartition, 10, done -> {

      if (done.succeeded()) {
        System.out.println("Seeking done");
      }
    });

    // seeking at the beginning of the partition
    consumer.seekToBeginning(Collections.singleton(topicPartition), done -> {

      if (done.succeeded()) {
        System.out.println("Seeking done");
      }
    });

    // seeking at the end of the partition
    consumer.seekToEnd(Collections.singleton(topicPartition), done -> {

      if (done.succeeded()) {
        System.out.println("Seeking done");
      }
    });
  }

  /**
   * Example about how Kafka consumer can pause reading from a topic partition
   * and then resume read operation for continuing to get messages
   * @param vertx
   * @param consumer
   */
  public void example8(Vertx vertx, KafkaConsumer<String, String> consumer) {

    Set<TopicPartition> topicPartitions = new HashSet<>();
    topicPartitions.add(new TopicPartition()
      .setTopic("test")
      .setPartition(0));

    // registering the handler for incoming messages
    consumer.handler(record -> {
      System.out.println("key=" + record.key() + ",value=" + record.value() +
        ",partition=" + record.partition() + ",offset=" + record.offset());

      // i.e. pause/resume on partition 0, after reading message up to offset 5
      if ((record.partition() == 0) && (record.offset() == 5)) {

        // pausing read operation
        consumer.pause(topicPartitions, done -> {

          if (done.succeeded()) {

            System.out.println("Paused");
            // resuming read operation after a specific time
            vertx.setTimer(5000, t -> {

              // resuming read operation
/*
              consumer.resume(topicPartitions, done1 -> {

                if (done1.succeeded()) {
                  System.out.println("Resumed");
                }
              });
*/

            });

          }
        });
      }
    });

    // subscribing to the topic
    consumer.subscribe(Collections.singleton("test"), done -> {

      if (done.succeeded()) {
        System.out.println("Consumer subscribed");
      }
    });
  }

  /**
   * Example about how Kafka producer sends message to topic
   * partitions in a round robin fashion
   * @param producer
   */
  public void example9(KafkaProducer<String, String> producer) {

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
  public void example10(KafkaProducer<String, String> producer) {

    for (int i = 0; i < 10; i++) {

      // a destination partition is specified
      KafkaProducerRecord<String, String> record =
        KafkaProducerRecord.create("test", null, "message_" + i, 0);

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
   * Example about how Kafka producer sends messages
   * specifying a key hashed internally for defining
   * the destination partition
   * @param producer
   */
  public void example11(KafkaProducer<String, String> producer) {

    for (int i = 0; i < 10; i++) {

      // i.e. defining different keys for odd and even messages
      int key = i % 2;

      // a key is specified, all messages with same key will be sent to the same partition
      KafkaProducerRecord<String, String> record =
        KafkaProducerRecord.create("test", String.valueOf(key), "message_" + i);

      producer.write(record, done -> {

        if (done.succeeded()) {

          RecordMetadata recordMetadata = done.result();
          System.out.println("Message " + record.value() + " written on topic=" + record.value() +
            ", partition=" + record.value() +
            ", offset=" + recordMetadata.getOffset());
        }

      });
    }

  }

  /**
   * Example about how Kafka client (producer or consumer)
   * can handle errors and exception during communication
   * with the Kafka cluster
   * @param consumer
   */
  public void example12(KafkaConsumer<String, String> consumer) {

    // setting handler for errors
    consumer.exceptionHandler(e -> {
      System.out.println("Error = " + e.getMessage());
    });
  }
}
