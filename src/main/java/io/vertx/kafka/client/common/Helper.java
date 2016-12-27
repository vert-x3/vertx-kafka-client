package io.vertx.kafka.client.common;

import io.vertx.core.Handler;
import io.vertx.kafka.client.producer.RecordMetadata;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class Helper {

  public static org.apache.kafka.common.TopicPartition to(TopicPartition topicPartition) {
    return new org.apache.kafka.common.TopicPartition(topicPartition.getTopic(), topicPartition.getPartition());
  }

  public static Collection<org.apache.kafka.common.TopicPartition> to(Collection<TopicPartition> topicPartitions) {
    return topicPartitions.stream().map(Helper::to).collect(Collectors.toList());
  }

  public static TopicPartition from(org.apache.kafka.common.TopicPartition topicPartition) {
    return new TopicPartition().setTopic(topicPartition.topic()).setPartition(topicPartition.partition());
  }

  public static Set<TopicPartition> from(Collection<org.apache.kafka.common.TopicPartition> topicPartitions) {
    return topicPartitions.stream().map(Helper::from).collect(Collectors.toSet());
  }

  public static Handler<Collection<org.apache.kafka.common.TopicPartition>> adaptHandler(Handler<Set<TopicPartition>> handler) {
    if (handler != null) {
      return topicPartitions -> handler.handle(Helper.from(topicPartitions));
    } else {
      return null;
    }
  }

  public static Node from(org.apache.kafka.common.Node node) {
    return new Node()
      .setHasRack(node.hasRack())
      .setHost(node.host())
      .setId(node.id())
      .setIdString(node.idString())
      .setIsEmpty(node.isEmpty())
      .setPort(node.port())
      .setRack(node.rack());
  }

  public static RecordMetadata from(org.apache.kafka.clients.producer.RecordMetadata metadata) {
    return new RecordMetadata()
      .setChecksum(metadata.checksum())
      .setOffset(metadata.offset())
      .setPartition(metadata.partition())
      .setTimestamp(metadata.timestamp())
      .setTopic(metadata.topic());
  }
}
