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
import io.vertx.kafka.client.common.TopicPartitionInfo;

import java.util.List;

/**
 * A detailed description of a single topic in the cluster
 */
@DataObject(generateConverter = true)
public class TopicDescription {

  private boolean isInternal;
  private String name;
  private List<TopicPartitionInfo> partitions;

  /**
   * Constructor
   */
  public TopicDescription() {

  }

  /**
   * Constructor
   *
   * @param isInternal Whether the topic is internal to Kafka.
   * @param name The name of the topic.
   * @param partitions A list of partitions where the index represents the partition id and the element
   *                   contains leadership and replica information for that partition.
   */
  public TopicDescription(boolean isInternal, String name, List<TopicPartitionInfo> partitions) {
    this.isInternal = isInternal;
    this.name = name;
    this.partitions = partitions;
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public TopicDescription(JsonObject json) {

    TopicDescriptionConverter.fromJson(json, this);
  }

  /**
   * @return Whether the topic is internal to Kafka.
   */
  public boolean isInternal() {
    return isInternal;
  }

  /**
   * Set whether the topic is internal to Kafka.
   *
   * @param internal Whether the topic is internal to Kafka.
   * @return current instance of the class to be fluent
   */
  public TopicDescription setInternal(boolean internal) {
    isInternal = internal;
    return this;
  }

  /**
   * @return The name of the topic.
   */
  public String getName() {
    return name;
  }

  /**
   * Set the name of the topic.
   *
   * @param name The name of the topic.
   * @return current instance of the class to be fluent
   */
  public TopicDescription setName(String name) {
    this.name = name;
    return this;
  }

  /**
   * @return A list of partitions where the index represents the partition id and the element
   * contains leadership and replica information for that partition.
   */
  public List<TopicPartitionInfo> getPartitions() {
    return partitions;
  }

  /**
   * Set A list of partitions where the index represents the partition id and the element
   * contains leadership and replica information for that partition.
   *
   * @param partitions A list of partitions where the index represents the partition id and the element
   *                   contains leadership and replica information for that partition.
   * @return current instance of the class to be fluent
   */
  public TopicDescription setPartitions(List<TopicPartitionInfo> partitions) {
    this.partitions = partitions;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {

    JsonObject json = new JsonObject();
    TopicDescriptionConverter.toJson(this, json);
    return json;
  }

  @Override
  public String toString() {

    return "TopicDescription{" +
      "isInternal=" + this.isInternal +
      ",name=" + this.name +
      ",partitions=" + this.partitions +
      "}";
  }
}
