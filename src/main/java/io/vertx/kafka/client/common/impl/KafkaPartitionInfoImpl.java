package io.vertx.kafka.client.common.impl;

import io.vertx.kafka.client.common.KafkaNode;
import io.vertx.kafka.client.common.KafkaPartitionInfo;
import org.apache.kafka.common.PartitionInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:ppatierno@live.com">Paolo Patierno</a>
 */
public class KafkaPartitionInfoImpl implements KafkaPartitionInfo {

  private final PartitionInfo partitionInfo;

  private List<KafkaNode> inSyncReplicas;
  private KafkaNode leader;
  private List<KafkaNode> replicas;

  /**
   * Constructor
   *
   * @param partitionInfo Kafka partition info for backing information
   */
  public KafkaPartitionInfoImpl(PartitionInfo partitionInfo) {
    this.partitionInfo = partitionInfo;

    this.inSyncReplicas = new ArrayList<>();
    for (int i = 0; i < partitionInfo.inSyncReplicas().length; i++) {
      this.inSyncReplicas.add(new KafkaNodeImpl(partitionInfo.inSyncReplicas()[i]));
    }

    this.leader = new KafkaNodeImpl(partitionInfo.leader());

    this.replicas = new ArrayList<>();
    for (int i = 0; i < partitionInfo.replicas().length; i++) {
      this.replicas.add(new KafkaNodeImpl(partitionInfo.replicas()[i]));
    }
  }

  @Override
  public List<KafkaNode> inSyncReplicas() {
    return  this.inSyncReplicas;
  }

  @Override
  public KafkaNode leader() {
    return this.leader;
  }

  @Override
  public int partition() {
    return this.partitionInfo.partition();
  }

  @Override
  public List<KafkaNode> replicas() {
    return this.replicas;
  }

  @Override
  public String topic() {
    return this.partitionInfo.topic();
  }

  @Override
  public PartitionInfo partitionInfo() {
    return this.partitionInfo;
  }
}
