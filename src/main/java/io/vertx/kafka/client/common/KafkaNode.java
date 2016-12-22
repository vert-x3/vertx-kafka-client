package io.vertx.kafka.client.common;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import org.apache.kafka.common.Node;

/**
 * @author <a href="mailto:ppatierno@live.com">Paolo Patierno</a>
 */
@VertxGen
public interface KafkaNode {

  /**
   * True if this node has a defined rack
   *
   * @return
   */
  boolean hasRack();

  /**
   * The host name for this node
   *
   * @return
   */
  String host();

  /**
   * The node id of this node
   *
   * @return
   */
  int id();

  /**
   * String representation of the node id
   *
   * @return
   */
  String idString();

  /**
   * Check whether this node is empty, which may be the case if noNode() is used as a placeholder in a response payload with an error
   *
   * @return
   */
  boolean isEmpty();

  /**
   * The port for this node
   *
   * @return
   */
  int port();

  /**
   * The rack for this node
   *
   * @return
   */
  String rack();

  /**
   * The Kafka node with backed information
   *
   * @return
   */
  @GenIgnore
  Node node();
}
