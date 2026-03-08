/*
 * Copyright 2026 Red Hat Inc.
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
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.common.TopicPartition;
import java.util.Collection;

@DataObject
@JsonGen(publicConverter = false)
public class ListConsumerGroupOffsetsSpec {

  private Collection<TopicPartition> topicPartitions = null;

  /**
   * Constructor
   */
  public ListConsumerGroupOffsetsSpec() {}

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public ListConsumerGroupOffsetsSpec(JsonObject json) {
    ListConsumerGroupOffsetsSpecConverter.fromJson(json, this);
  }

  /**
   * Set the topic partitions to list as part of the result.
   * {@code null} includes all topic partitions.
   *
   * @param topicPartitions Collection of topic partitions to include
   * @return This ListConsumerGroupOffsetsSpec
   */
  public ListConsumerGroupOffsetsSpec topicPartitions(Collection<TopicPartition> topicPartitions) {
    this.topicPartitions = topicPartitions;
    return this;
  }

  /**
   * Returns a collection of topic partitions to add as part of the result.
   */
  public Collection<TopicPartition> topicPartitions() {
    return topicPartitions;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    ListConsumerGroupOffsetsSpecConverter.toJson(this, json);
    return json;
  }

  @Override
  public String toString() {
    return "ListConsumerGroupOffsetsSpec{" +
      "topicPartitions=" + topicPartitions +
      '}';
  }

}
