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

/**
 * A listing of a consumer group in the cluster.
 */
@DataObject(generateConverter = true)
public class ConsumerGroupListing {

  private String groupId;
  private boolean isSimpleConsumerGroup;

  /**
   * Constructor
   */
  public ConsumerGroupListing() {

  }

  /**
   * Constructor
   *
   * @param groupId consumer group id
   * @param isSimpleConsumerGroup if consumer group is simple or not
   */
  public ConsumerGroupListing(String groupId, boolean isSimpleConsumerGroup) {
    this.groupId = groupId;
    this.isSimpleConsumerGroup = isSimpleConsumerGroup;
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public ConsumerGroupListing(JsonObject json) {

    ConsumerGroupListingConverter.fromJson(json, this);
  }

  /**
   * @return consumer group id
   */
  public String getGroupId() {
    return groupId;
  }

  /**
   * Set the consumer group id
   *
   * @param groupId consumer group id
   * @return current instance of the class to be fluent
   */
  public ConsumerGroupListing setGroupId(String groupId) {
    this.groupId = groupId;
    return this;
  }

  /**
   * @return if consumer group is simple or not
   */
  public boolean isSimpleConsumerGroup() {
    return isSimpleConsumerGroup;
  }

  /**
   * Set if consumer group is simple or not
   *
   * @param isSimpleConsumerGroup if consumer group is simple or not
   * @return current instance of the class to be fluent
   */
  public ConsumerGroupListing setSimpleConsumerGroup(boolean isSimpleConsumerGroup) {
    this.isSimpleConsumerGroup = isSimpleConsumerGroup;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {

    JsonObject json = new JsonObject();
    ConsumerGroupListingConverter.toJson(this, json);
    return json;
  }

  @Override
  public String toString() {

    return "ConsumerGroupListing{" +
      "groupId=" + this.groupId +
      ",isSimpleConsumerGroup=" + this.isSimpleConsumerGroup +
      "}";
  }
}
