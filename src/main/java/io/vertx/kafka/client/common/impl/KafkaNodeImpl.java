package io.vertx.kafka.client.common.impl;

import io.vertx.kafka.client.common.KafkaNode;
import org.apache.kafka.common.Node;

/**
 * @author <a href="mailto:ppatierno@live.com">Paolo Patierno</a>
 */
public class KafkaNodeImpl implements KafkaNode {

  private final Node node;

  /**
   * Constructor
   *
   * @param node  Kafka node for backing information
   */
  public KafkaNodeImpl(Node node) {
    this.node = node;
  }

  @Override
  public boolean hasRack() {
    return this.node.hasRack();
  }

  @Override
  public String host() {
    return this.node.host();
  }

  @Override
  public int id() {
    return this.node.id();
  }

  @Override
  public String idString() {
    return this.node.idString();
  }

  @Override
  public boolean isEmpty() {
    return this.node.isEmpty();
  }

  @Override
  public int port() {
    return this.node.port();
  }

  @Override
  public String rack() {
    return this.node.rack();
  }

  @Override
  public Node node() {
    return this.node;
  }
}
