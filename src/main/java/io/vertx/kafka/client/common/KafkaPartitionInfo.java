package io.vertx.kafka.client.common;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;

/**
 * @author <a href="mailto:ppatierno@live.com">Paolo Patierno</a>
 */
@VertxGen
public interface KafkaPartitionInfo {

  /**
   * The subset of the replicas that are in sync, that is caught-up to the leader and ready to take over as leader if the leader should fail
   *
   * @return
   */
  List<KafkaNode> inSyncReplicas();

  /**
   * The node id of the node currently acting as a leader for this partition or null if there is no leader
   *
   * @return
   */
  KafkaNode leader();

  /**
   * The partition id
   *
   * @return
   */
  int partition();

  /**
   * The complete set of replicas for this partition regardless of whether they are alive or up-to-date
   *
   * @return
   */
  List<KafkaNode> replicas();

  /**
   * The topic name
   *
   * @return
   */
  String topic();

  /**
   * The Kafka partition info with backed information
   *
   * @return
   */
  @GenIgnore
  PartitionInfo partitionInfo();
}
