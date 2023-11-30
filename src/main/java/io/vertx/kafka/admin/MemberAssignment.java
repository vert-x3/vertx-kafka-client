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
import io.vertx.codegen.annotations.JsonGen;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.common.TopicPartition;

import java.util.Set;

/**
 * A description of the assignments of a specific group member
 */
@DataObject
@JsonGen(publicConverter = false)
public class MemberAssignment {

  private Set<TopicPartition> topicPartitions;

  /**
   * Constructor
   */
  public MemberAssignment() {

  }

  /**
   * Constructor
   *
   * @param topicPartitions list of topic partitions
   */
  public MemberAssignment(Set<TopicPartition> topicPartitions) {
    this.topicPartitions = topicPartitions;
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public MemberAssignment(JsonObject json) {

    MemberAssignmentConverter.fromJson(json, this);
  }

  /**
   * @return list of topic partitions
   */
  public Set<TopicPartition> getTopicPartitions() {
    return topicPartitions;
  }

  /**
   * Set the list of topic partitions
   *
   * @param topicPartitions list of topic partitions
   * @return current instance of the class to be fluent
   */
  public MemberAssignment setTopicPartitions(Set<TopicPartition> topicPartitions) {
    this.topicPartitions = topicPartitions;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {

    JsonObject json = new JsonObject();
    MemberAssignmentConverter.toJson(this, json);
    return json;
  }

  @Override
  public String toString() {

    return "MemberAssignment{" +
      "topicPartitions=" + this.topicPartitions +
      "}";
  }
}
