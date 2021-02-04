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

package io.vertx.kafka.client.common.impl;

import io.vertx.core.Handler;
import io.vertx.kafka.admin.Config;
import io.vertx.kafka.admin.ConfigEntry;
import io.vertx.kafka.admin.ConsumerGroupListing;
import io.vertx.kafka.admin.ListConsumerGroupOffsetsOptions;
import io.vertx.kafka.admin.ListOffsetsResultInfo;
import io.vertx.kafka.admin.MemberAssignment;
import io.vertx.kafka.admin.NewTopic;
import io.vertx.kafka.admin.OffsetSpec;
import io.vertx.kafka.client.common.ConfigResource;
import io.vertx.kafka.client.common.Node;
import io.vertx.kafka.client.consumer.OffsetAndTimestamp;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.kafka.clients.admin.AlterConfigOp;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helper class for mapping native and Vert.x Kafka objects
 */
public class Helper {

  private Helper() {
  }

  public static <T> Set<T> toSet(Collection<T> collection) {
    if (collection instanceof Set) {
      return (Set<T>) collection;
    } else {
      return new HashSet<>(collection);
    }
  }

  public static org.apache.kafka.common.TopicPartition to(TopicPartition topicPartition) {
    return new org.apache.kafka.common.TopicPartition(topicPartition.getTopic(), topicPartition.getPartition());
  }

  public static Set<org.apache.kafka.common.TopicPartition> to(Set<TopicPartition> topicPartitions) {
    return topicPartitions.stream().map(Helper::to).collect(Collectors.toSet());
  }

  public static Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> to(Map<TopicPartition, OffsetAndMetadata> offsets) {
    return offsets.entrySet().stream().collect(Collectors.toMap(
      e -> new org.apache.kafka.common.TopicPartition(e.getKey().getTopic(), e.getKey().getPartition()),
      e -> new org.apache.kafka.clients.consumer.OffsetAndMetadata(e.getValue().getOffset(), e.getValue().getMetadata()))
    );
  }

  public static Map<TopicPartition, OffsetAndMetadata> from(Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets) {
    return offsets.entrySet().stream().collect(Collectors.toMap(
      e -> new TopicPartition(e.getKey().topic(), e.getKey().partition()),
      e -> new OffsetAndMetadata(e.getValue().offset(), e.getValue().metadata()))
    );
  }

  public static TopicPartition from(org.apache.kafka.common.TopicPartition topicPartition) {
    return new TopicPartition(topicPartition.topic(), topicPartition.partition());
  }

  public static Set<TopicPartition> from(Collection<org.apache.kafka.common.TopicPartition> topicPartitions) {
    return topicPartitions.stream().map(Helper::from).collect(Collectors.toSet());
  }

  public static Handler<Set<org.apache.kafka.common.TopicPartition>> adaptHandler(Handler<Set<TopicPartition>> handler) {
    if (handler != null) {
      return topicPartitions -> handler.handle(Helper.from(topicPartitions));
    } else {
      return null;
    }
  }

  public static Node from(org.apache.kafka.common.Node node) {
    return new Node(node.hasRack(), node.host(), node.id(), node.idString(),
      node.isEmpty(), node.port(), node.rack());
  }

  public static RecordMetadata from(org.apache.kafka.clients.producer.RecordMetadata metadata) {
    return new RecordMetadata(metadata.checksum(), metadata.offset(),
      metadata.partition(), metadata.timestamp(), metadata.topic());
  }

  public static OffsetAndMetadata from(org.apache.kafka.clients.consumer.OffsetAndMetadata offsetAndMetadata) {
    if (offsetAndMetadata != null) {
      return new OffsetAndMetadata(offsetAndMetadata.offset(), offsetAndMetadata.metadata());
    } else {
      return null;
    }
  }

  public static org.apache.kafka.clients.consumer.OffsetAndMetadata to(OffsetAndMetadata offsetAndMetadata) {
    return new org.apache.kafka.clients.consumer.OffsetAndMetadata(offsetAndMetadata.getOffset(), offsetAndMetadata.getMetadata());
  }

  public static Map<TopicPartition, Long> fromTopicPartitionOffsets(Map<org.apache.kafka.common.TopicPartition, Long> offsets) {
    return offsets.entrySet().stream().collect(Collectors.toMap(
      e -> new TopicPartition(e.getKey().topic(), e.getKey().partition()),
      Map.Entry::getValue)
    );
  }

  public static Map<org.apache.kafka.common.TopicPartition, Long> toTopicPartitionTimes(Map<TopicPartition, Long> topicPartitionTimes) {
    return topicPartitionTimes.entrySet().stream().collect(Collectors.toMap(
      e -> new org.apache.kafka.common.TopicPartition(e.getKey().getTopic(), e.getKey().getPartition()),
      Map.Entry::getValue)
    );
  }

  public static Map<TopicPartition, OffsetAndTimestamp> fromTopicPartitionOffsetAndTimestamp(Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> topicPartitionOffsetAndTimestamps) {
    return topicPartitionOffsetAndTimestamps.entrySet().stream()
      .filter(e-> e.getValue() != null)
      .collect(Collectors.toMap(
        e -> new TopicPartition(e.getKey().topic(), e.getKey().partition()),
        e ->new OffsetAndTimestamp(e.getValue().offset(), e.getValue().timestamp()))
      );
  }

  public static org.apache.kafka.clients.admin.NewTopic to(NewTopic topic) {
    org.apache.kafka.clients.admin.NewTopic newTopic = null;
    if (topic.getNumPartitions() != -1 && topic.getReplicationFactor() != -1) {
      newTopic = new org.apache.kafka.clients.admin.NewTopic(topic.getName(), topic.getNumPartitions(), topic.getReplicationFactor());
    } else {
      newTopic = new org.apache.kafka.clients.admin.NewTopic(topic.getName(), topic.getReplicasAssignments());
    }
    if (topic.getConfig() != null && !topic.getConfig().isEmpty()) {
      newTopic.configs(topic.getConfig());
    }
    return newTopic;
  }

  public static org.apache.kafka.common.config.ConfigResource to(ConfigResource configResource) {
    return new org.apache.kafka.common.config.ConfigResource(configResource.getType(), configResource.getName());
  }

  public static ConfigResource from(org.apache.kafka.common.config.ConfigResource configResource) {
    return new ConfigResource(configResource.type(), configResource.name());
  }

  public static Config from(org.apache.kafka.clients.admin.Config config) {
    return new Config(Helper.fromConfigEntries(config.entries()));
  }

  public static List<org.apache.kafka.clients.admin.NewTopic> toNewTopicList(List<NewTopic> topics) {
    return topics.stream().map(Helper::to).collect(Collectors.toList());
  }

  public static List<org.apache.kafka.common.config.ConfigResource> toConfigResourceList(List<ConfigResource> configResources) {
    return configResources.stream().map(Helper::to).collect(Collectors.toList());
  }

  public static org.apache.kafka.clients.admin.ConfigEntry to(ConfigEntry configEntry) {
    return new org.apache.kafka.clients.admin.ConfigEntry(configEntry.getName(), configEntry.getValue());
  }

  public static Map<org.apache.kafka.common.config.ConfigResource, Collection<AlterConfigOp>> toConfigMaps(Map<ConfigResource, Config> configs) {

    return configs.entrySet().stream().collect(Collectors.toMap(
            e -> new org.apache.kafka.common.config.ConfigResource(e.getKey().getType(), e.getKey().getName()),
            e -> e.getValue().getEntries().stream().map(
                    v -> new AlterConfigOp(to(v), AlterConfigOp.OpType.SET)).collect(Collectors.toList())));

  }

  public static ConfigEntry from(org.apache.kafka.clients.admin.ConfigEntry configEntry) {
    return new ConfigEntry(configEntry.name(), configEntry.value());
  }

  public static List<ConfigEntry> fromConfigEntries(Collection<org.apache.kafka.clients.admin.ConfigEntry> configEntries) {
    return configEntries.stream().map(Helper::from).collect(Collectors.toList());
  }

  public static ConsumerGroupListing from(org.apache.kafka.clients.admin.ConsumerGroupListing consumerGroupListing) {
    return new ConsumerGroupListing(consumerGroupListing.groupId(), consumerGroupListing.isSimpleConsumerGroup());
  }

  public static List<ConsumerGroupListing> fromConsumerGroupListings(Collection<org.apache.kafka.clients.admin.ConsumerGroupListing> consumerGroupListings) {
    return consumerGroupListings.stream().map(Helper::from).collect(Collectors.toList());
  }

  public static MemberAssignment from(org.apache.kafka.clients.admin.MemberAssignment memberAssignment) {
    return new MemberAssignment(Helper.from(memberAssignment.topicPartitions()));
  }

  public static org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions to(ListConsumerGroupOffsetsOptions listConsumerGroupOffsetsOptions) {

    org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions newListConsumerGroupOffsetsOptions = new org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions();

    if (listConsumerGroupOffsetsOptions.topicPartitions() != null) {
      List<org.apache.kafka.common.TopicPartition> topicPartitions = listConsumerGroupOffsetsOptions.topicPartitions().stream()
        .map(tp -> new org.apache.kafka.common.TopicPartition(tp.getTopic(), tp.getPartition()))
        .collect(Collectors.toList());

      newListConsumerGroupOffsetsOptions.topicPartitions(topicPartitions);
    }

    return newListConsumerGroupOffsetsOptions;
  }

  public static Set<org.apache.kafka.common.TopicPartition> toTopicPartitionSet(Set<TopicPartition> partitions) {
    return partitions.stream().map(Helper::to).collect(Collectors.toSet());
  }

  public static org.apache.kafka.clients.admin.OffsetSpec to(OffsetSpec os) {
    if (os.EARLIEST == os) {
      return org.apache.kafka.clients.admin.OffsetSpec.earliest();
    } else if (os.LATEST == os) {
      return org.apache.kafka.clients.admin.OffsetSpec.latest();
    } else {
      return org.apache.kafka.clients.admin.OffsetSpec.forTimestamp(os.getSpec());
    }
  }

  public static Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> toTopicPartitionOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets) {
    return topicPartitionOffsets.entrySet().stream().collect(Collectors.toMap(
      e -> new org.apache.kafka.common.TopicPartition(e.getKey().getTopic(), e.getKey().getPartition()),
      e -> to(e.getValue())
    ));
  }

  public static ListOffsetsResultInfo from(org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo lori) {
    return new ListOffsetsResultInfo(lori.offset(), lori.timestamp(), lori.leaderEpoch().orElse(null));
  }
}
