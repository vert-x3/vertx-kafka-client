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
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
  public void example2(KafkaConsumer<?,?> consumer) {

    // registering the handler for incoming messages
    consumer.handler(record -> {
      System.out.println(record.key() + " " + record.value());
    });

    // registering handlers for assigned and revoked partitions
    consumer.partitionsAssignedHandler(topicPartitions -> {

      System.out.println("Partitions assigned");
      topicPartitions.stream().forEach(topicPartition -> {
        System.out.println(topicPartition.getTopic() + " " + topicPartition.getPartition());
      });
    });

    consumer.partitionsRevokedHandler(topicPartitions -> {

      System.out.println("Partitions revoked");
      topicPartitions.stream().forEach(topicPartition -> {
        System.out.println(topicPartition.getTopic() + " " + topicPartition.getPartition());
      });
    });

    // subscribing to the topic
    consumer.subscribe(Collections.singleton("test"), done -> {

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
  public void example3(KafkaConsumer<?,?> consumer) {

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
  public void example4(KafkaConsumer<?,?> consumer) {

    Set<TopicPartition> topicPartitions = new HashSet<>();
    topicPartitions.add(new TopicPartition().setTopic("test").setPartition(0));

    // registering the handler for incoming messages
    consumer.handler(record -> {
      System.out.println(record.key() + " " + record.value());
    });

    // requesting to be assigned the specific partition
    consumer.assign(topicPartitions, done -> {

      if (done.succeeded()) {
        System.out.println("Partition assigned");

        // requesting the assigned partitions
        consumer.assignment(done1 -> {

          if (done1.succeeded()) {

            done1.result().stream().forEach(topicPartition -> {
              System.out.println(topicPartition.getTopic() + " " + topicPartition.getPartition());
            });
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
  public void example5(KafkaConsumer<?,?> consumer) {

    // asking information about available topics and related partitions
    consumer.listTopics(done -> {

      if (done.succeeded()) {

        done.result().forEach((topic, partitions) -> {

          System.out.println("topic = " + topic);
          System.out.println("partitions = " + partitions);
        });
      }
    });

    // asking partitions information about specific topic
    consumer.partitionsFor("test", done -> {

      if (done.succeeded()) {

        done.result().stream().forEach(partitionInfo -> {
          System.out.println(partitionInfo);
        });
      }
    });
  }

  /**
   * Example about how Kafka consumer can handle manual commit
   * of the current offset for a topic partition
   * @param consumer
   */
  public void example6(KafkaConsumer<?,?> consumer) {

    // consumer is processing read messages

    // committing offset of the last read message
    consumer.commit(done -> {

      if (done.succeeded()) {
        System.out.println("Last read message offset committed");
      }
    });
  }
}
