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

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.core.Future;
import io.vertx.kafka.client.common.ConfigResource;
import io.vertx.kafka.client.common.TopicPartition;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.RecordsToDelete;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.admin.impl.KafkaAdminClientImpl;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Vert.x Kafka Admin client implementation
 */
@VertxGen
public interface KafkaAdminClient {

  /**
   * Create a new KafkaAdminClient instance
   *
   * @param vertx Vert.x instance to use
   * @param adminClient Kafka native Admin client instance
   * @return an instance of the KafkaAdminClient
   */
  @GenIgnore
  static KafkaAdminClient create(Vertx vertx, AdminClient adminClient) {
    return new KafkaAdminClientImpl(vertx, adminClient);
  }

  /**
   * Create a new KafkaAdminClient instance
   *
   * @param vertx Vert.x instance to use
   * @param config Kafka admin client configuration
   * @return an instance of the KafkaAdminClient
   */
  static KafkaAdminClient create(Vertx vertx, Map<String, String> config) {
    return create(vertx, AdminClient.create(new HashMap<>(config)));
  }

  /**
   * Create a new KafkaAdminClient instance
   *
   * @param vertx Vert.x instance to use
   * @param config Kafka admin client configuration
   * @return an instance of the KafkaAdminClient
   */
  @GenIgnore
  static KafkaAdminClient create(Vertx vertx, Properties config) {
    return create(vertx, AdminClient.create(config));
  }

  /**
   * List the topics available in the cluster with the default options.
   *
   * @param completionHandler handler called on operation completed with the topics set
   */
  @Deprecated
  void listTopics(Handler<AsyncResult<Set<String>>> completionHandler);

  /**
   * Like {@link #listTopics(Handler)} but returns a {@code Future} of the asynchronous result
   */
  Future<Set<String>> listTopics();

  /**
   * Describe some topics in the cluster, with the default options.
   *
   * @param topicNames the names of the topics to describe
   * @param completionHandler handler called on operation completed with the topics descriptions
   */
  @Deprecated
  void describeTopics(List<String> topicNames, Handler<AsyncResult<Map<String, TopicDescription>>> completionHandler);

    /**
   * Like {@link #describeTopics(List, Handler)} but allows for customised otions
   */
    @Deprecated
  void describeTopics(List<String> topicNames, DescribeTopicsOptions options, Handler<AsyncResult<Map<String, TopicDescription>>> completionHandler);

  /**
   * Like {@link #describeTopics(List, Handler)} but returns a {@code Future} of the asynchronous result
   */
  Future<Map<String, TopicDescription>> describeTopics(List<String> topicNames);

  /**
   * Like {@link #describeTopics(List, DescribeTopicsOptions, Handler)} but returns a {@code Future} of the asynchronous result
   */
  Future<Map<String, TopicDescription>> describeTopics(List<String> topicNames, DescribeTopicsOptions options);

  /**
   * Creates a batch of new Kafka topics
   *
   * @param topics topics to create
   * @param completionHandler handler called on operation completed
   */
  @Deprecated
  void createTopics(List<NewTopic> topics, Handler<AsyncResult<Void>> completionHandler);

  /**
   * Like {@link #createTopics(List, Handler)} but returns a {@code Future} of the asynchronous result
   */
  Future<Void> createTopics(List<NewTopic> topics);

  /**
   * Deletes a batch of Kafka topics
   *
   * @param topicNames the names of the topics to delete
   * @param completionHandler handler called on operation completed
   */
  @Deprecated
  void deleteTopics(List<String> topicNames, Handler<AsyncResult<Void>> completionHandler);

  /**
   * Like {@link #deleteTopics(List, Handler)} but returns a {@code Future} of the asynchronous result
   */
  Future<Void> deleteTopics(List<String> topicNames);

  /**
   * Creates a batch of new partitions in the Kafka topic
   *
   * @param partitions partitions to create
   * @param completionHandler handler called on operation completed
   */
  @Deprecated
  void createPartitions(Map<String, io.vertx.kafka.admin.NewPartitions> partitions, Handler<AsyncResult<Void>> completionHandler);

  /**
   * Like {@link #createPartitions(Map, Handler)} but returns a {@code Future} of the asynchronous result
   * @param partitions
   */
  Future<Void> createPartitions(Map<String, io.vertx.kafka.admin.NewPartitions> partitions);


  /**
   * Get the configuration for the specified resources with the default options
   *
   * @param configResources the resources (topic and broker resource types are currently supported)
   * @param completionHandler handler called on operation completed with the configurations
   */
  @GenIgnore
  void describeConfigs(List<ConfigResource> configResources, Handler<AsyncResult<Map<ConfigResource, Config>>> completionHandler);

  /**
   * Like {@link #describeConfigs(List, Handler)} but returns a {@code Future} of the asynchronous result
   */
  @GenIgnore
  Future<Map<ConfigResource, Config>> describeConfigs(List<ConfigResource> configResources);

  /**
   * Update the configuration for the specified resources with the default options
   *
   * @param configs The resources with their configs (topic is the only resource type with configs that can be updated currently)
   * @param completionHandler handler called on operation completed
   */
  @GenIgnore
  void alterConfigs(Map<ConfigResource,Config> configs, Handler<AsyncResult<Void>> completionHandler);

  /**
   * Like {@link #alterConfigs(Map, Handler)} but returns a {@code Future} of the asynchronous result
   */
  @GenIgnore
  Future<Void> alterConfigs(Map<ConfigResource,Config> configs);

  /**
   * Get the the consumer groups available in the cluster with the default options
   *
   * @param completionHandler handler called on operation completed with the consumer groups ids
   */
  @Deprecated
  void listConsumerGroups(Handler<AsyncResult<List<ConsumerGroupListing>>> completionHandler);

  /**
   * Like {@link #listConsumerGroups(Handler)} but returns a {@code Future} of the asynchronous result
   */
  Future<List<ConsumerGroupListing>> listConsumerGroups();

  /**
   * Describe some group ids in the cluster, with the default options
   *
   * @param groupIds the ids of the groups to describe
   * @param completionHandler handler called on operation completed with the consumer groups descriptions
   */
  @Deprecated
  void describeConsumerGroups(List<String> groupIds, Handler<AsyncResult<Map<String, ConsumerGroupDescription>>> completionHandler);

  /**
   * Like {@link #describeConsumerGroups(List, Handler)} but returns a {@code Future} of the asynchronous result
   */
  Future<Map<String, ConsumerGroupDescription>> describeConsumerGroups(List<String> groupIds);

  /**
   * Like {@link #describeConsumerGroups(List, Handler)} but allows customized options
   */
  @Deprecated
  void describeConsumerGroups(List<String> groupIds, DescribeConsumerGroupsOptions options, Handler<AsyncResult<Map<String, ConsumerGroupDescription>>> completionHandler);

  /**
   * Like {@link #describeConsumerGroups(List, DescribeConsumerGroupsOptions, Handler)} but returns a {@code Future} of the asynchronous result
   */
  Future<Map<String, ConsumerGroupDescription>> describeConsumerGroups(List<String> groupIds, DescribeConsumerGroupsOptions options);


  /**
   * Describe the nodes in the cluster with the default options
   *
   * @param completionHandler handler called on operation completed with the cluster description
   */
  @Deprecated
  void describeCluster(Handler<AsyncResult<ClusterDescription>> completionHandler);

  /**
   * Like {@link #describeCluster(Handler)} but returns a {@code Future} of the asynchronous result
   */
  Future<ClusterDescription> describeCluster();

  /**
   * Like {@link #describeCluster(Handler)} but allows customized options.
   */
  @Deprecated
  void describeCluster(DescribeClusterOptions options, Handler<AsyncResult<ClusterDescription>> completionHandler);

  /**
   * Like {@link #describeCluster(DescribeClusterOptions, Handler)} but returns a {@code Future} of the asynchronous result
   */
  Future<ClusterDescription> describeCluster(DescribeClusterOptions options);

  /**
   * Query the information of all log directories on the given set of brokers
   *
   * @param brokers list of broker ids
   * @param completionHandler a {@code Handler} completed with the operation result
   */
  @GenIgnore
  void describeLogDirs(List<Integer> brokers, Handler<AsyncResult<Map<Integer, Map<String, LogDirDescription>>>> completionHandler);

  /**
   * Like {@link #describeLogDirs(List, Handler)} but returns a {@code Future} of the asynchronous result
   */
  @GenIgnore
  Future<Map<Integer, Map<String, LogDirDescription>>> describeLogDirs(final List<Integer> brokers);

  /**
   * Delete records from a topic partition.
   *
   * @param recordsToDelete records to be delted on the given  topic partition
   * @param completionHandler handler called on operation completed
   */
  @GenIgnore
  void deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete,Handler<AsyncResult<Map<TopicPartition, DeletedRecords>>> completionHandler);

  /**
   * Like {@link #deleteRecords(Map, Handler)} but returns a {@code Future} of the asynchronous result
   */
  @GenIgnore
  Future<Map<TopicPartition, DeletedRecords>> deleteRecords(final Map<TopicPartition, RecordsToDelete> recordsToDelete);

  /**
   * Delete consumer groups from the cluster.
   *
   * @param groupIds the ids of the groups to delete
   * @param completionHandler handler called on operation completed
   */
  @Deprecated
  void deleteConsumerGroups(List<String> groupIds, Handler<AsyncResult<Void>> completionHandler);

  /**
   * Like {@link #deleteConsumerGroups(List, Handler)} but returns a {@code Future} of the asynchronous result
   */
  Future<Void> deleteConsumerGroups(List<String> groupIds);

  /**
   * List the consumer group offsets available in the cluster.
   *
   * @param groupId The group id of the group whose offsets will be listed
   * @param options The options to use when listing the consumer group offsets.
   * @param completionHandler handler called on operation completed with the consumer groups offsets
   */
  @GenIgnore
  @Deprecated
  void listConsumerGroupOffsets(String groupId, ListConsumerGroupOffsetsOptions options, Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>> completionHandler);

  /**
   * Like {@link #listConsumerGroupOffsets(String, ListConsumerGroupOffsetsOptions, Handler)} but returns a {@code Future} of the asynchronous result
   */
  @GenIgnore
  Future<Map<TopicPartition, OffsetAndMetadata>> listConsumerGroupOffsets(String groupId, ListConsumerGroupOffsetsOptions options);

  /**
   * List the consumer group offsets available in the cluster.
   *
   * @param groupId The group id of the group whose offsets will be listed
   * @param completionHandler handler called on operation completed with the consumer groups offsets
   */
  @GenIgnore
  @Deprecated
  default void listConsumerGroupOffsets(String groupId, Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>> completionHandler) {
    listConsumerGroupOffsets(groupId, new ListConsumerGroupOffsetsOptions(), completionHandler);
  }

  /**
   * Like {@link #listConsumerGroupOffsets(String, Handler)} but returns a {@code Future} of the asynchronous result
   */
  @GenIgnore
  default Future<Map<TopicPartition, OffsetAndMetadata>> listConsumerGroupOffsets(String groupId) {
    return listConsumerGroupOffsets(groupId, new ListConsumerGroupOffsetsOptions());
  }

  /**
   * Delete committed offsets for a set of partitions in a consumer group. This will
   * succeed at the partition level only if the group is not actively subscribed
   * to the corresponding topic.
   *
   * @param groupId The group id of the group whose offsets will be deleted
   * @param partitions The set of partitions in the consumer group whose offsets will be deleted
   */
  @Deprecated
  void deleteConsumerGroupOffsets(String groupId, Set<TopicPartition> partitions, Handler<AsyncResult<Void>> completionHandler);

  /**
   * Like {@link #deleteConsumerGroupOffsets(String, Set, Handler)} but returns a {@code Future} of the asynchronous result
   */
  Future<Void> deleteConsumerGroupOffsets(String groupId, Set<TopicPartition> partitions);

  /**
   * Alter committed offsets for a set of partitions in a consumer group.
   *
   * @param groupId The group id of the group whose offsets will be altered
   * @param offsets The map of offsets in the consumer group which will be altered
   */
  @GenIgnore
  @Deprecated
  void alterConsumerGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets, Handler<AsyncResult<Void>> completionHandler);

  /**
   * Like {@link #alterConsumerGroupOffsets(String, Map, Handler)} but returns a {@code Future} of the asynchronous result
   */
  @GenIgnore
  Future<Void> alterConsumerGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets);

  /**
   * List the offsets available for a set of partitions.
   *
   * @param topicPartitionOffsets The options to use when listing the partition offsets.
   * @param completionHandler handler called on operation completed with the partition offsets
   */
  @GenIgnore
  @Deprecated
  void listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets, Handler<AsyncResult<Map<TopicPartition, ListOffsetsResultInfo>>> completionHandler);

  /**
   * Like {@link #listOffsets(Map<TopicPartition, OffsetSpec>, Handler)} but returns a {@code Future} of the asynchronous result
   */
  @GenIgnore
  Future<Map<TopicPartition, ListOffsetsResultInfo>> listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets);

  /**
   * Describe the ACL rules.
   *
   * @param aclBindingFilter The filter to use.
   * @param completionHandler handler called on operation completed with the ACL description result.
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  @Deprecated
  void describeAcls(AclBindingFilter aclBindingFilter, Handler<AsyncResult<List<AclBinding>>> completionHandler);

  /**
   * Like {@link #describeAcls(AclBindingFilter, Handler)} but returns a {@code Future} of the asynchronous result
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  Future<List<AclBinding>> describeAcls(AclBindingFilter aclBindingFilter);

  /**
   * Create the ACL rules.
   *
   * @param aclBindings The ACL to create.
   * @param completionHandler handler called on operation completed with the ACL creation result.
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  @Deprecated
  void createAcls(List<AclBinding> aclBindings, Handler<AsyncResult<List<AclBinding>>> completionHandler);

  /**
   * Like {@link #createAcls(List)} (List<AclBinding>, Handler)} but returns a {@code Future} of the asynchronous result
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  Future<List<AclBinding>> createAcls(List<AclBinding> aclBindings);

  /**
   * Delete the ACL rules.
   *
   * @param aclBindings The filter to delete matching ACLs.
   * @param completionHandler handler called on operation completed with the ACL deletion result.
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  @Deprecated
  void deleteAcls(List<AclBindingFilter> aclBindings, Handler<AsyncResult<List<AclBinding>>> completionHandler);

  /**
   * Like {@link #deleteAcls(List)} (List<AclBinding>, Handler)} but returns a {@code Future} of the asynchronous result
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  Future<List<AclBinding>> deleteAcls(List<AclBindingFilter> aclBindings);

  /**
   * Close the admin client
   *
   * @param handler a {@code Handler} completed with the operation result
   */
  @Deprecated
  void close(Handler<AsyncResult<Void>> handler);

  /**
   * Like {@link #close(Handler)} but returns a {@code Future} of the asynchronous result
   */
  Future<Void> close();

  /**
   * Close the admin client
   *
   * @param timeout timeout to wait for closing
   * @param handler a {@code Handler} completed with the operation result
   */
  @Deprecated
  void close(long timeout, Handler<AsyncResult<Void>> handler);

  /**
   * Like {@link #close(long, Handler)} but returns a {@code Future} of the asynchronous result
   */
  Future<Void> close(long timeout);
}
