/*
 * Copyright (c) 2011-2019 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */

package examples;

import io.vertx.core.Vertx;
import io.vertx.docgen.Source;
import io.vertx.kafka.client.consumer.AcknowledgeType;
import io.vertx.kafka.client.consumer.KafkaShareConsumer;
import io.vertx.kafka.client.consumer.KafkaShareConsumerRecord;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Source
public class KafkaShareConsumerExamples {

  /**
   * Example about Kafka share consumer creation
   * @param vertx Vert.x instance
   */
  public void exampleCreateShareConsumer(Vertx vertx) {
    // creating the share consumer using map config
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    // the share group this consumer joins, there is no auto.offset.reset for share groups
    config.put("group.id", "my_share_group");

    // use the share consumer for interacting with Apache Kafka
    KafkaShareConsumer<String, String> consumer = KafkaShareConsumer.create(vertx, config);
  }

  /**
   * Example about creating a share consumer using explicit acknowledgement
   * @param vertx Vert.x instance
   */
  public void exampleCreateShareConsumerExplicitAcknowledgement(Vertx vertx) {
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("group.id", "my_share_group");
    // opt in to explicit acknowledgement, records are acknowledged one by one
    config.put("share.acknowledgement.mode", "explicit");

    KafkaShareConsumer<String, String> consumer = KafkaShareConsumer.create(vertx, config);
  }

  /**
   * Example about subscribing to a topic with a share consumer
   * @param consumer Kafka share consumer instance
   */
  public void exampleShareConsumerSubscribe(KafkaShareConsumer<String, String> consumer) {
    consumer.handler(record -> {
      System.out.println("key=" + record.key() + ",value=" + record.value() +
        ",partition=" + record.partition() + ",offset=" + record.offset());
    });

    consumer
      .subscribe("test")
      .onSuccess(v -> System.out.println("Share consumer subscribed"))
      .onFailure(cause -> System.out.println("Could not subscribe " + cause.getMessage()));
  }

  /**
   * Example about acknowledging records explicitly
   * @param consumer Kafka share consumer instance, configured with explicit acknowledgement
   */
  public void exampleShareConsumerAcknowledge(KafkaShareConsumer<String, String> consumer) {
    consumer.handler(record ->
      // the record was handled successfully, it will not be delivered again
      consumer
        .acknowledge(record, AcknowledgeType.ACCEPT)
        .onFailure(cause -> System.out.println("Could not acknowledge " + cause.getMessage()))
    );

    consumer.subscribe("test");
  }

  /**
   * Example about choosing the acknowledgement type based on the processing outcome
   * @param consumer Kafka share consumer instance, configured with explicit acknowledgement
   */
  public void exampleShareConsumerAcknowledgeTypes(KafkaShareConsumer<String, String> consumer) {
    consumer.handler(record -> {
      try {
        process(record);
        // processed, never delivered again
        consumer.acknowledge(record, AcknowledgeType.ACCEPT);
      } catch (TransientFailure e) {
        // could not process now, make it available again for any consumer of the share group
        consumer.acknowledge(record, AcknowledgeType.RELEASE);
      } catch (Exception e) {
        // the record can never be processed, do not deliver it again to anyone
        consumer.acknowledge(record, AcknowledgeType.REJECT);
      }
    });

    consumer.subscribe("test");
  }

  /**
   * Example about using the delivery count to give up on a poison record
   * @param consumer Kafka share consumer instance, configured with explicit acknowledgement
   */
  public void exampleShareConsumerDeliveryCount(KafkaShareConsumer<String, String> consumer) {
    consumer.handler(record -> {
      try {
        process(record);
        consumer.acknowledge(record, AcknowledgeType.ACCEPT);
      } catch (Exception e) {
        if (record.deliveryCount() >= 5) {
          // give up after five attempts, the record is not redelivered to any consumer
          consumer.acknowledge(record, AcknowledgeType.REJECT);
        } else {
          consumer.acknowledge(record, AcknowledgeType.RELEASE);
        }
      }
    });

    consumer.subscribe("test");
  }

  /**
   * Example about committing acknowledgements synchronously
   * @param consumer Kafka share consumer instance
   */
  public void exampleShareConsumerCommitSync(KafkaShareConsumer<String, String> consumer) {
    consumer.handler(record ->
      consumer
        .acknowledge(record, AcknowledgeType.ACCEPT)
        // send the pending acknowledgements to the broker and wait for the result
        .compose(v -> consumer.commitSync())
        .onSuccess(v -> System.out.println("Acknowledgements committed"))
        .onFailure(cause -> System.out.println("Commit failed " + cause.getMessage()))
    );

    consumer.subscribe("test");
  }

  /**
   * Example about committing acknowledgements asynchronously
   * @param consumer Kafka share consumer instance
   */
  public void exampleShareConsumerCommitAsync(KafkaShareConsumer<String, String> consumer) {
    consumer.handler(record ->
      consumer
        .acknowledge(record, AcknowledgeType.ACCEPT)
        // does not wait for the broker, the result is delivered to the commit callback
        .onSuccess(v -> consumer.commitAsync())
    );

    consumer.subscribe("test");
  }

  /**
   * Example about being notified of the result of the asynchronous commits.
   * The callback is invoked once per broker response and reports the offsets
   * of the records whose acknowledgements were confirmed.
   * @param consumer Kafka share consumer instance
   */
  public void exampleShareConsumerCommitCallback(KafkaShareConsumer<String, String> consumer) {
    consumer.setAcknowledgementCommitCallback((offsets, exception) -> {
      if (exception != null) {
        System.out.println("Acknowledgements failed to commit " + exception.getMessage());
      } else {
        offsets.forEach((partition, acknowledged) ->
          System.out.println("Committed " + acknowledged.size() +
            " acknowledgement(s) for " + partition));
      }
    });

    consumer.handler(record ->
      consumer
        .acknowledge(record, AcknowledgeType.ACCEPT)
        .onSuccess(v -> consumer.commitAsync())
    );

    consumer.subscribe("test");
  }

  /**
   * Example about receiving messages with explicit polling
   * @param vertx Vert.x instance
   * @param consumer Kafka share consumer instance
   */
  public void exampleShareConsumerWithPoll(Vertx vertx, KafkaShareConsumer<String, String> consumer) {
    consumer
      .subscribe("test")
      .onSuccess(v -> {
        System.out.println("Share consumer subscribed");

        // start polling right after subscribing, and keep polling to stay in the group
        vertx.setPeriodic(1000, timerId ->
          consumer
            .poll(Duration.ofMillis(100))
            .onSuccess(records -> {
              for (int i = 0; i < records.size(); i++) {
                KafkaShareConsumerRecord<String, String> record = records.recordAt(i);
                System.out.println("key=" + record.key() + ",value=" + record.value() +
                  ",partition=" + record.partition() + ",offset=" + record.offset());
              }
            })
            .onFailure(cause -> {
              System.out.println("Something went wrong when polling " + cause.getMessage());
              vertx.cancelTimer(timerId);
            })
        );
      });
  }

  /**
   * Example about closing a share consumer
   * @param consumer Kafka share consumer instance
   */
  public void exampleShareConsumerClose(KafkaShareConsumer<String, String> consumer) {
    consumer
      .close()
      .onSuccess(v -> System.out.println("Share consumer is now closed"))
      .onFailure(cause -> System.out.println("Close failed " + cause.getMessage()));
  }

  private void process(KafkaShareConsumerRecord<String, String> record) throws TransientFailure {
  }

  private static class TransientFailure extends Exception {
  }
}
