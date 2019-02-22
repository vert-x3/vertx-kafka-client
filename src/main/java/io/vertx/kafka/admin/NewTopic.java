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

import java.util.List;
import java.util.Map;

/**
 * A new topic to be created
 */
@DataObject(generateConverter = true)
public class NewTopic {

  private String name;
  private int numPartitions;
  private short replicationFactor;
  private Map<Integer, List<Integer>> replicasAssignments;
  private Map<String,String> config;

  /**
   * Constructor
   */
  public NewTopic() {

  }

  /**
   * Constructor
   *
   * @param name the topic name
   * @param numPartitions number of partitions
   * @param replicationFactor replication factor
   */
  public NewTopic(String name, int numPartitions, short replicationFactor) {
    this.name = name;
    this.numPartitions = numPartitions;
    this.replicationFactor = replicationFactor;
  }

  /**
   * Constructor
   *
   * @param name the topic name
   * @param replicasAssignments a map from partition id to replica ids
   */
  public NewTopic(java.lang.String name,
                  java.util.Map<Integer, List<Integer>> replicasAssignments) {
    this.name = name;
    this.replicasAssignments = replicasAssignments;
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public NewTopic(JsonObject json) {

    NewTopicConverter.fromJson(json, this);
  }

  /**
   * @return the name of the topic to be created
   */
  public String getName() {
    return name;
  }

  /**
   * Set the name of the topic to be created
   *
   * @param name the name of the topic to be created
   * @return current instance of the class to be fluent
   */
  public NewTopic setName(String name) {
    this.name = name;
    return this;
  }

  /**
   * @return the number of partitions for the new topic or -1 if a replica assignment has been specified
   */
  public int getNumPartitions() {
    return numPartitions;
  }

  /**
   * Set the number of partitions for the new topic or -1 if a replica assignment has been specified
   *
   * @param numPartitions the number of partitions for the new topic or -1 if a replica assignment has been specified
   * @return current instance of the class to be fluent
   */
  public NewTopic setNumPartitions(int numPartitions) {
    this.numPartitions = numPartitions;
    return this;
  }

  /**
   * @return the replication factor for the new topic or -1 if a replica assignment has been specified
   */
  public short getReplicationFactor() {
    return replicationFactor;
  }

  /**
   * Set the replication factor for the new topic or -1 if a replica assignment has been specified
   *
   * @param replicationFactor the replication factor for the new topic or -1 if a replica assignment has been specified
   * @return current instance of the class to be fluent
   */
  public NewTopic setReplicationFactor(short replicationFactor) {
    this.replicationFactor = replicationFactor;
    return this;
  }

  /**
   * @return a map from partition id to replica ids
   */
  public Map<Integer, List<Integer>> getReplicasAssignments() {
    return replicasAssignments;
  }

  /**
   * Set a map from partition id to replica ids
   *
   * @param replicasAssignments a map from partition id to replica ids
   * @return current instance of the class to be fluent
   */
  public NewTopic setReplicasAssignments(Map<Integer, List<Integer>> replicasAssignments) {
    this.replicasAssignments = replicasAssignments;
    return this;
  }

  /**
   * @return the configuration for the new topic or null if no configs ever specified
   */
  public Map<String, String> getConfig() {
    return config;
  }

  /**
   * Set the configuration for the new topic or null if no configs ever specified
   *
   * @param config the configuration for the new topic or null if no configs ever specified
   * @return current instance of the class to be fluent
   */
  public NewTopic setConfig(Map<String, String> config) {
    this.config = config;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {

    JsonObject json = new JsonObject();
    NewTopicConverter.toJson(this, json);
    return json;
  }

  @Override
  public String toString() {

    return "NewTopic{" +
      "name=" + this.name +
      ",numPartitions=" + this.numPartitions +
      ",replicationFactor=" + this.replicationFactor +
      ",replicasAssignments=" + this.replicasAssignments +
      ",config=" + this.config +
      "}";
  }
}
