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

@DataObject(generateConverter = true)
public class ListOffsetsResultInfo {
  private long offset;
  private long timestamp;
  private Integer leaderEpoch;

  /**
   * Constructor
   *
   * @param offset the offset
   * @param timestamp the timestamp
   * @param leaderEpoch the leader epoch
   */
  public ListOffsetsResultInfo(long offset, long timestamp, Integer leaderEpoch) {
    this.offset = offset;
    this.timestamp = timestamp;
    this.leaderEpoch = leaderEpoch;
  }


  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public ListOffsetsResultInfo(JsonObject json) {
    ListOffsetsResultInfoConverter.fromJson(json, this);
  }

  /**
   * @return the offset
   */
  public long getOffset() {
    return offset;
  }

  /**
   * @return the timestamp
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * @return the leader epoch
   */
  public Integer getLeaderEpoch() {
    return leaderEpoch;
  }

  /**
   * Set the offset
   *
   * @param offset the offset
   * @return current instance of the class to be fluent
   */
  public ListOffsetsResultInfo setOffset(long offset) {
    this.offset = offset;
    return this;
  }
  /**
   * Set the timestamp
   *
   * @param timestamp the timestamp
   * @return current instance of the class to be fluent
   */
  public ListOffsetsResultInfo setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }
  /**
   * Set the leader epoch
   *
   * @param leaderEpoch the leader epoch
   * @return current instance of the class to be fluent
   */
  public ListOffsetsResultInfo setLeaderEpoch(Integer leaderEpoch) {
    this.leaderEpoch = leaderEpoch;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {

    JsonObject json = new JsonObject();
    ListOffsetsResultInfoConverter.toJson(this, json);
    return json;
  }

  @Override
  public String toString() {
    return "ListOffsetsResultInfo{" +
      "offset=" + this.offset +
      ",timestamp=" + this.timestamp +
      (this.leaderEpoch != null ? ",leaderEpoch=" + this.leaderEpoch : "") +
      "}";
  }
}
