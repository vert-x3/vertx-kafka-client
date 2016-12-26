package io.vertx.kafka.client.common;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;

/**
 * @author <a href="mailto:ppatierno@live.com">Paolo Patierno</a>
 */
@DataObject(generateConverter = true)
public class PartitionInfo {

  private List<Node> inSyncReplicas;
  private Node leader;
  private int partition;
  private List<Node> replicas;
  private String topic;

  /**
   * Constructor
   */
  public PartitionInfo() {

  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public PartitionInfo(JsonObject json) {

    PartitionInfoConverter.fromJson(json, this);
  }

  /**
   * The subset of the replicas that are in sync, that is caught-up to the leader and ready to take over as leader if the leader should fail
   *
   * @return
   */
  public List<Node> inSyncReplicas() {
    return this.inSyncReplicas;
  }

  /**
   * Set the subset of the replicas that are in sync
   *
   * @param inSyncReplicas  the subset of the replicas that are in sync
   * @return  current instance of the class to be fluent
   */
  public PartitionInfo setInSyncReplicas(List<Node> inSyncReplicas) {
    this.inSyncReplicas = inSyncReplicas;
    return this;
  }

  /**
   * The node id of the node currently acting as a leader for this partition or null if there is no leader
   *
   * @return
   */
  public Node leader() {
    return this.leader;
  }

  /**
   * Set the node id of the node currently acting as a leader
   *
   * @param leader  the node id of the node currently acting as a leader
   * @return  current instance of the class to be fluent
   */
  public PartitionInfo setLeader(Node leader) {
    this.leader = leader;
    return this;
  }

  /**
   * The partition id
   *
   * @return
   */
  public int partition() {
    return this.partition;
  }

  /**
   * Set the partition id
   *
   * @param partition the partition id
   * @return  current instance of the class to be fluent
   */
  public PartitionInfo setPartition(int partition) {
    this.partition = partition;
    return this;
  }

  /**
   * The complete set of replicas for this partition regardless of whether they are alive or up-to-date
   *
   * @return
   */
  public List<Node> replicas() {
    return this.replicas;
  }

  /**
   * Set the complete set of replicas for this partition
   *
   * @param replicas  the complete set of replicas for this partition
   * @return  current instance of the class to be fluent
   */
  public PartitionInfo setReplicas(List<Node> replicas) {
    this.replicas = replicas;
    return this;
  }

  /**
   * The topic name
   *
   * @return
   */
  public String topic() {
    return this.topic;
  }

  /**
   * Set the topic name
   *
   * @param topic the topic name
   * @return  current instance of the class to be fluent
   */
  public PartitionInfo setTopic(String topic) {
    this.topic = topic;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {
    JsonObject jsonObject = new JsonObject();

    if (this.inSyncReplicas != null) {
      JsonArray inSyncReplicas = new JsonArray();
      this.inSyncReplicas.forEach(node -> inSyncReplicas.add(node.toJson()));
    }

    if (this.replicas != null) {
      JsonArray replicas = new JsonArray();
      this.replicas.forEach(node -> replicas.add(node.toJson()));
    }

    jsonObject
      .put("inSyncReplicas", inSyncReplicas)
      .put("leader", this.leader.toJson())
      .put("partition", this.partition)
      .put("replicas", replicas)
      .put("topic", this.topic);

    return jsonObject;
  }
}
