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
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.acl.AclOperation;

import java.util.List;
import java.util.Set;

/**
 * A detailed description of a single consumer group in the cluster
 */
@DataObject(generateConverter = true)
public class ConsumerGroupDescription  {

  private String groupId;
  private boolean isSimpleConsumerGroup;
  private Node coordinator;
  private List<MemberDescription> members;
  private String partitionAssignor;
  private ConsumerGroupState state;
  private Set<AclOperation> authorizedOperations;

  /**
   * Constructor
   */
  public ConsumerGroupDescription() {

  }

  /**
   * Constructor
   *
   * @param groupId the id of the consumer group
   * @param isSimpleConsumerGroup if consumer group is simple or not
   * @param members a list of the members of the consumer group
   * @param partitionAssignor the consumer group partition assignor
   * @param state the consumer group state, or UNKNOWN if the state is too new for us to parse
   * @param coordinator the consumer group coordinator, or null if the coordinator is not known
   * @param authorizedOperations authorizedOperations for this group, or null if that information is not known.
   */
  public ConsumerGroupDescription(String groupId, boolean isSimpleConsumerGroup, List<MemberDescription> members,
                                  String partitionAssignor, ConsumerGroupState state, Node coordinator,
                                  Set<AclOperation> authorizedOperations) {
    this.groupId = groupId;
    this.isSimpleConsumerGroup = isSimpleConsumerGroup;
    this.members = members;
    this.partitionAssignor = partitionAssignor;
    this.state = state;
    this.coordinator = coordinator;
    this.authorizedOperations = authorizedOperations;
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public ConsumerGroupDescription(JsonObject json) {

    ConsumerGroupDescriptionConverter.fromJson(json, this);
  }

  /**
   * @return the id of the consumer group
   */
  public String getGroupId() {
    return groupId;
  }

  /**
   * Set the id of the consumer group
   *
   * @param groupId the id of the consumer group
   * @return current instance of the class to be fluent
   */
  public ConsumerGroupDescription setGroupId(String groupId) {
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
  public ConsumerGroupDescription setSimpleConsumerGroup(boolean isSimpleConsumerGroup) {
    this.isSimpleConsumerGroup = isSimpleConsumerGroup;
    return this;
  }

  /**
   * @return the consumer group coordinator, or null if the coordinator is not known
   */
  public Node getCoordinator() {
    return coordinator;
  }

  /**
   * Set the consumer group coordinator, or null if the coordinator is not known
   *
   * @param coordinator the consumer group coordinator, or null if the coordinator is not known
   * @return current instance of the class to be fluent
   */
  public ConsumerGroupDescription setCoordinator(Node coordinator) {
    this.coordinator = coordinator;
    return this;
  }

  /**
   * @return a list of the members of the consumer group
   */
  public List<MemberDescription> getMembers() {
    return members;
  }

  /**
   * Set a list of the members of the consumer group
   *
   * @param members a list of the members of the consumer group
   * @return current instance of the class to be fluent
   */
  public ConsumerGroupDescription setMembers(List<MemberDescription> members) {
    this.members = members;
    return this;
  }

  /**
   * @return the consumer group partition assignor
   */
  public String getPartitionAssignor() {
    return partitionAssignor;
  }

  /**
   * Set the consumer group partition assignor
   *
   * @param partitionAssignor the consumer group partition assignor
   * @return current instance of the class to be fluent
   */
  public ConsumerGroupDescription setPartitionAssignor(String partitionAssignor) {
    this.partitionAssignor = partitionAssignor;
    return this;
  }

  /**
   * @return the consumer group state, or UNKNOWN if the state is too new for us to parse
   */
  public ConsumerGroupState getState() {
    return state;
  }

  /**
   * Set the consumer group state, or UNKNOWN if the state is too new for us to parse
   *
   * @param state the consumer group state, or UNKNOWN if the state is too new for us to parse
   * @return current instance of the class to be fluent
   */
  public ConsumerGroupDescription setState(ConsumerGroupState state) {
    this.state = state;
    return this;
  }

  /**
   * @return authorizedOperations authorizedOperations for this group, or null if that information is not known.
   */
  public Set<AclOperation> getAuthorizedOperations() {
    return this.authorizedOperations;
  }

  /**
   * Set the id of the consumer group
   *
   * @param authorizedOperations authorizedOperations for this group, or null if that information is not known.
   * @return current instance of the class to be fluent
   */
  public ConsumerGroupDescription setAuthorizedOperations(Set<AclOperation> authorizedOperations) {
    this.authorizedOperations = authorizedOperations;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {

    JsonObject json = new JsonObject();
    ConsumerGroupDescriptionConverter.toJson(this, json);
    return json;
  }

  @Override
  public String toString() {

    return "ConsumerGroupDescription{" +
      "groupId=" + this.groupId +
      ",isSimpleConsumerGroup=" + this.isSimpleConsumerGroup +
      ",coordinator=" + this.coordinator +
      ",members=" + this.members +
      ",partitionAssignor=" + this.partitionAssignor +
      ",state=" + this.state +
      ",authorizedOperations=" + this.authorizedOperations +
      "}";
  }
}
