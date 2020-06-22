/*
 * Copyright 2019 Red Hat Inc.
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

package io.vertx.kafka.admin;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.common.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A detailed description of the cluster
 */
@DataObject(generateConverter = true)
public class ClusterDescription {
  private String clusterId;
  private Node controller;
  private List<Node> nodes;

  /**
   * Constructor
   */
  public ClusterDescription() {

  }

  /**
   * Constructor
   *
   * @param clusterId The cluster ID.
   * @param controller The controller node.
   * @param nodes A collection of nodes belonging to this cluster.
   */
  public ClusterDescription(String clusterId, Node controller, List<Node> nodes) {
    this.clusterId = clusterId;
    this.controller = controller;
    this.nodes = nodes;
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public ClusterDescription(JsonObject json) {

    ClusterDescriptionConverter.fromJson(json, this);
  }

  /**
   *
   * @return the nodes belonging to this cluster.
   */
  public List<Node> getNodes() {
    return nodes;
  }

  /**
   * Set the nodes belonging to this cluster
   *
   * @param nodes the nodes
   * @return current instance of the class to be fluent
   */
  public ClusterDescription setNodes(List<Node> nodes) {
    this.nodes = nodes;
    return this;
  }

  /**
   * Add a node belonging to this cluster to the current node list.
   *
   * @param node the node to add
   * @return current instance of the class to be fluent
   */
  public ClusterDescription addNode(Node node) {
    Objects.requireNonNull(node, "Cannot accept null node");
    if (nodes == null) {
      nodes = new ArrayList<>();
    }
    nodes.add(node);
    return this;
  }

  /**
   *
   * @return the controller node.
   */
  public Node getController() {
    return controller;
  }

  /**
   * Set the controller node.
   *
   * @param controller the controller node
   * @return current instance of the class to be fluent
   */
  public ClusterDescription setController(Node controller) {
    this.controller = controller;
    return this;
  }

  /**
   *
   * @return the cluster ID
   */
  public String getClusterId() {
    return clusterId;
  }

  /**
   * Set the cluster ID
   *
   * @param clusterId
   * @return current instance of the class to be fluent
   */
  public ClusterDescription setClusterId(String clusterId) {
    this.clusterId = clusterId;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {

    JsonObject json = new JsonObject();
    ClusterDescriptionConverter.toJson(this, json);
    return json;
  }

  @Override
  public String toString() {

    return "ClusterDescription{" +
      "clusterId=" + this.clusterId +
      ",controller=" + this.controller +
      ",nodes=" + this.nodes +
      "}";
  }
}
