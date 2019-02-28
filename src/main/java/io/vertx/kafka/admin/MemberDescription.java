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
 * A detailed description of a single group instance in the cluster
 */
@DataObject(generateConverter = true)
public class MemberDescription {

  private String consumerId;
  private String clientId;
  private MemberAssignment assignment;
  private String host;

  /**
   * Constructor
   */
  public MemberDescription() {

  }

  /**
   * Constructor
   *
   * @param consumerId the consumer id of the group member
   * @param clientId the client id of the group member
   * @param host the host where the group member is running
   * @param assignment the assignment of the group member
   */
  public MemberDescription(String consumerId, String clientId, String host, MemberAssignment assignment) {
    this.consumerId = consumerId;
    this.clientId = clientId;
    this.host = host;
    this.assignment = assignment;
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public MemberDescription(JsonObject json) {

    MemberDescriptionConverter.fromJson(json, this);
  }

  /**
   * @return the consumer id of the group member
   */
  public String getConsumerId() {
    return consumerId;
  }

  /**
   * Set the consumer id of the group member
   *
   * @param consumerId the consumer id of the group member
   * @return current instance of the class to be fluent
   */
  public MemberDescription setConsumerId(String consumerId) {
    this.consumerId = consumerId;
    return this;
  }

  /**
   * @return the client id of the group member
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * Set the client id of the group member
   *
   * @param clientId the client id of the group member
   * @return current instance of the class to be fluent
   */
  public MemberDescription setClientId(String clientId) {
    this.clientId = clientId;
    return this;
  }

  /**
   * @return the assignment of the group member
   */
  public MemberAssignment getAssignment() {
    return assignment;
  }

  /**
   * Set the assignment of the group member
   *
   * @param assignment the assignment of the group member
   * @return current instance of the class to be fluent
   */
  public MemberDescription setAssignment(MemberAssignment assignment) {
    this.assignment = assignment;
    return this;
  }

  /**
   * @return the host where the group member is running
   */
  public String getHost() {
    return host;
  }

  /**
   * Set the host where the group member is running
   *
   * @param host the host where the group member is running
   * @return current instance of the class to be fluent
   */
  public MemberDescription setHost(String host) {
    this.host = host;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {

    JsonObject json = new JsonObject();
    MemberDescriptionConverter.toJson(this, json);
    return json;
  }

  @Override
  public String toString() {

    return "MemberDescription{" +
      "consumerId=" + this.consumerId +
      ",clientId=" + this.clientId +
      ",assignment=" + this.assignment +
      ",host=" + this.host +
      "}";
  }
}
