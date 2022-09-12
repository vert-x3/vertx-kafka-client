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

package io.vertx.kafka.admin.impl;

import io.vertx.kafka.admin.ListConsumerGroupOffsetsOptions;
import io.vertx.kafka.admin.NewPartitions;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import io.vertx.core.Promise;
import io.vertx.core.impl.ContextInternal;
import io.vertx.kafka.admin.ClusterDescription;
import io.vertx.kafka.admin.Config;
import io.vertx.kafka.admin.ConsumerGroupDescription;
import io.vertx.kafka.admin.ConsumerGroupListing;
import io.vertx.kafka.admin.ListOffsetsResultInfo;
import io.vertx.kafka.admin.MemberDescription;
import io.vertx.kafka.admin.NewTopic;
import io.vertx.kafka.admin.OffsetSpec;
import io.vertx.kafka.admin.TopicDescription;
import io.vertx.kafka.admin.DescribeClusterOptions;
import io.vertx.kafka.admin.DescribeConsumerGroupsOptions;
import io.vertx.kafka.admin.DescribeTopicsOptions;
import io.vertx.kafka.client.common.ConfigResource;
import io.vertx.kafka.client.common.Node;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.common.TopicPartitionInfo;
import io.vertx.kafka.client.common.impl.Helper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.RecordsToDelete;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.admin.KafkaAdminClient;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;

public class KafkaAdminClientImpl implements KafkaAdminClient {

  private Vertx vertx;
  private AdminClient adminClient;

  public KafkaAdminClientImpl(Vertx vertx, AdminClient adminClient) {
      this.vertx = vertx;
      this.adminClient = adminClient;
  }

  @Override
  public void describeTopics(List<String> topicNames, Handler<AsyncResult<Map<String, TopicDescription>>> completionHandler) {
    describeTopics(topicNames).onComplete(completionHandler);
  }

  @Override
  public Future<Map<String, TopicDescription>> describeTopics(List<String> topicNames) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<Map<String, TopicDescription>> promise = ctx.promise();

    DescribeTopicsResult describeTopicsResult = this.adminClient.describeTopics(topicNames);
    describeTopicsResult.all().whenComplete((t, ex) -> {
      if (ex == null) {

        Map<String, TopicDescription> topics = new HashMap<>();

        for (Map.Entry<String, org.apache.kafka.clients.admin.TopicDescription> topicDescriptionEntry : t.entrySet()) {

          List<TopicPartitionInfo> partitions = new ArrayList<>();

          for (org.apache.kafka.common.TopicPartitionInfo kafkaPartitionInfo : topicDescriptionEntry.getValue().partitions()) {

            TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo();
            topicPartitionInfo.setIsr(
              kafkaPartitionInfo.isr().stream().map(Helper::from).collect(Collectors.toList()))
              .setLeader(Helper.from(kafkaPartitionInfo.leader()))
              .setPartition(kafkaPartitionInfo.partition())
              .setReplicas(
                kafkaPartitionInfo.replicas().stream().map(Helper::from).collect(Collectors.toList()));

            partitions.add(topicPartitionInfo);
          }

          TopicDescription topicDescription = new TopicDescription();

          topicDescription.setInternal(topicDescriptionEntry.getValue().isInternal())
            .setName(topicDescriptionEntry.getKey())
            .setPartitions(partitions)
            .setTopicId(topicDescriptionEntry.getValue().topicId())
            .setAuthorizedOperations(topicDescriptionEntry.getValue().authorizedOperations());

          topics.put(topicDescriptionEntry.getKey(), topicDescription);
        }

        promise.complete(topics);
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  @Override
  public void describeTopics(List<String> topicNames, DescribeTopicsOptions options, Handler<AsyncResult<Map<String, TopicDescription>>> completionHandler) {
    describeTopics(topicNames, options).onComplete(completionHandler);
  }

  @Override
  public Future<Map<String, TopicDescription>> describeTopics(List<String> topicNames, DescribeTopicsOptions options) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<Map<String, TopicDescription>> promise = ctx.promise();

    DescribeTopicsResult describeTopicsResult = this.adminClient.describeTopics(topicNames, Helper.to(options));
    describeTopicsResult.all().whenComplete((t, ex) -> {
      if (ex == null) {

        Map<String, TopicDescription> topics = new HashMap<>();

        for (Map.Entry<String, org.apache.kafka.clients.admin.TopicDescription> topicDescriptionEntry : t.entrySet()) {

          List<TopicPartitionInfo> partitions = new ArrayList<>();

          for (org.apache.kafka.common.TopicPartitionInfo kafkaPartitionInfo : topicDescriptionEntry.getValue().partitions()) {

            TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo();
            topicPartitionInfo.setIsr(
              kafkaPartitionInfo.isr().stream().map(Helper::from).collect(Collectors.toList()))
              .setLeader(Helper.from(kafkaPartitionInfo.leader()))
              .setPartition(kafkaPartitionInfo.partition())
              .setReplicas(
                kafkaPartitionInfo.replicas().stream().map(Helper::from).collect(Collectors.toList()));

            partitions.add(topicPartitionInfo);
          }

          TopicDescription topicDescription = new TopicDescription();

          topicDescription.setInternal(topicDescriptionEntry.getValue().isInternal())
            .setName(topicDescriptionEntry.getKey())
            .setPartitions(partitions)
            .setTopicId(topicDescriptionEntry.getValue().topicId())
            .setAuthorizedOperations(topicDescriptionEntry.getValue().authorizedOperations());

          topics.put(topicDescriptionEntry.getKey(), topicDescription);
        }

        promise.complete(topics);
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  @Override
  @GenIgnore
  public void describeLogDirs(List<Integer> brokers, Handler<AsyncResult<Map<Integer, Map<String, LogDirDescription>>>> completionHandler) {
    describeLogDirs(brokers).onComplete(completionHandler);
  }

  @Override
  @GenIgnore
  public Future<Map<Integer, Map<String, LogDirDescription>>> describeLogDirs(List<Integer> brokers) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<Map<Integer, Map<String, LogDirDescription>>> promise = ctx.promise();

    DescribeLogDirsResult describeLogDirsResult = this.adminClient.describeLogDirs(brokers);
    describeLogDirsResult.allDescriptions().whenComplete((t, ex) -> {
      if (ex == null) {

        Map<Integer, Map<String, LogDirDescription>> logDirsDescriptions = new HashMap<>();

        for (Map.Entry<Integer, Map<String, org.apache.kafka.clients.admin.LogDirDescription>> logDirsDescriptionsEntry : t.entrySet()) {

          Integer broker = logDirsDescriptionsEntry.getKey();

          Map<String, LogDirDescription> logDirsDescription = new HashMap<>();

          for (Map.Entry<String, org.apache.kafka.clients.admin.LogDirDescription> logDirsDescriptionEntry : logDirsDescriptionsEntry.getValue().entrySet()) {
            logDirsDescription.put(logDirsDescriptionEntry.getKey(),logDirsDescriptionEntry.getValue());
          }
          logDirsDescriptions.put(broker, logDirsDescription);
        }
        promise.complete(logDirsDescriptions);
     } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }


  @Override
  public void listTopics(Handler<AsyncResult<Set<String>>> completionHandler) {
    listTopics().onComplete(completionHandler);
  }

  @Override
  public Future<Set<String>> listTopics() {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<Set<String>> promise = ctx.promise();

    ListTopicsResult listTopicsResult = this.adminClient.listTopics();
    listTopicsResult.names().whenComplete((topics, ex) -> {
      if (ex == null) {
        promise.complete(topics);
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  @Override
  public void createTopics(List<NewTopic> topics, Handler<AsyncResult<Void>> completionHandler) {
    createTopics(topics).onComplete(completionHandler);
  }

  @Override
  public Future<Void> createTopics(List<NewTopic> topics) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<Void> promise = ctx.promise();

    CreateTopicsResult createTopicsResult = this.adminClient.createTopics(Helper.toNewTopicList(topics));
    createTopicsResult.all().whenComplete((v, ex) -> {

      if (ex == null) {
        promise.complete();
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  @Override
  public void deleteTopics(List<String> topicNames, Handler<AsyncResult<Void>> completionHandler) {
    deleteTopics(topicNames).onComplete(completionHandler);
  }

  @Override
  public Future<Void> deleteTopics(List<String> topicNames) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<Void> promise = ctx.promise();

    DeleteTopicsResult deleteTopicsResult = this.adminClient.deleteTopics(topicNames);
    deleteTopicsResult.all().whenComplete((v, ex) -> {

      if (ex == null) {
        promise.complete();
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  @Override
  @GenIgnore
  public void deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete,Handler<AsyncResult<Map<TopicPartition, DeletedRecords>>> completionHandler) {
    deleteRecords(recordsToDelete).onComplete(completionHandler);
  }

  @Override
  @GenIgnore
  public Future<Map<TopicPartition, DeletedRecords>> deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<Map<TopicPartition, DeletedRecords>> promise = ctx.promise();
    Map<org.apache.kafka.common.TopicPartition, RecordsToDelete> recordsToDeleteKafka = new HashMap<>();
    for(Map.Entry<TopicPartition, RecordsToDelete> entry : recordsToDelete.entrySet()){
      recordsToDeleteKafka.put(Helper.to(entry.getKey()),entry.getValue());
    }
    DeleteRecordsResult deleteRecordsResult = this.adminClient.deleteRecords(recordsToDeleteKafka);
    Map<org.apache.kafka.common.TopicPartition, KafkaFuture<DeletedRecords>> deletedRecordsInfo = deleteRecordsResult.lowWatermarks();
    Map<TopicPartition, DeletedRecords> deletedRecordsInfoMap = new HashMap<>();
    List<Future> deletedRecords = new ArrayList<>();
    List<org.apache.kafka.common.TopicPartition> topicPartitions = new ArrayList<>();
    for(Map.Entry<org.apache.kafka.common.TopicPartition, KafkaFuture<DeletedRecords>> entry : deletedRecordsInfo.entrySet()){
      Promise<DeletedRecords> promise1 = ctx.promise();
      topicPartitions.add(entry.getKey());
      deletedRecords.add(promise1.future());
      entry.getValue().whenComplete((dr, ex)-> {
        if (ex == null){
          promise1.complete();
        } else {
          promise1.fail(ex);
        }
      });
    }
    CompositeFuture.join(deletedRecords).onComplete((drs) -> {
      if(drs.failed()){
        promise.fail("Error message drs: " + drs);
        return;
      }
      List<DeletedRecords> recordList  = drs.result().list();
      for (int i=0 ; i < recordList.size() ; i++){
        deletedRecordsInfoMap.put(Helper.from(topicPartitions.get(i)), recordList.get(i));
      }
      promise.complete(deletedRecordsInfoMap);
    });
    return promise.future();
  }



  @Override
  public void createPartitions(Map<String, NewPartitions> partitions, Handler<AsyncResult<Void>> completionHandler) {
    createPartitions(partitions).onComplete(completionHandler);
  }

  @Override
  public Future<Void> createPartitions(Map<String, NewPartitions> partitions) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<Void> promise = ctx.promise();
    CreatePartitionsResult createPartitionsResult = this.adminClient.createPartitions(Helper.toPartitions(partitions));
    createPartitionsResult.all().whenComplete((v, ex) -> {

      if (ex == null) {
        promise.handle(Future.succeededFuture());
      } else {
        promise.handle(Future.failedFuture(ex));
      }
    });
    return promise.future();
  }


  @Override
  public void describeConfigs(List<ConfigResource> configResources, Handler<AsyncResult<Map<ConfigResource, Config>>> completionHandler) {
    describeConfigs(configResources).onComplete(completionHandler);
  }

  @Override
  public Future<Map<ConfigResource, Config>> describeConfigs(List<ConfigResource> configResources) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<Map<ConfigResource, Config>> promise = ctx.promise();

    DescribeConfigsResult describeConfigsResult = this.adminClient.describeConfigs(Helper.toConfigResourceList(configResources));
    describeConfigsResult.all().whenComplete((m, ex) -> {

      if (ex == null) {

        Map<ConfigResource, Config> configs = new HashMap<>();

        for (Map.Entry<org.apache.kafka.common.config.ConfigResource, org.apache.kafka.clients.admin.Config> configEntry : m.entrySet()) {

          ConfigResource configResource = Helper.from(configEntry.getKey());
          Config config = Helper.from(configEntry.getValue());

          configs.put(configResource, config);
        }

        promise.complete(configs);
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  @Override
  public void alterConfigs(Map<ConfigResource,Config> configs, Handler<AsyncResult<Void>> completionHandler) {
    alterConfigs(configs).onComplete(completionHandler);
  }

  @Override
  public Future<Void> alterConfigs(Map<ConfigResource, Config> configs) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<Void> promise = ctx.promise();

    AlterConfigsResult alterConfigsResult = this.adminClient.incrementalAlterConfigs(Helper.toConfigMaps(configs));
    alterConfigsResult.all().whenComplete((v, ex) -> {

      if (ex == null) {
        promise.complete();
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  @Override
  public void listConsumerGroups(Handler<AsyncResult<List<ConsumerGroupListing>>> completionHandler) {
    listConsumerGroups().onComplete(completionHandler);
  }

  @Override
  public Future<List<ConsumerGroupListing>> listConsumerGroups() {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<List<ConsumerGroupListing>> promise = ctx.promise();

    ListConsumerGroupsResult listConsumerGroupsResult = this.adminClient.listConsumerGroups();
    listConsumerGroupsResult.all().whenComplete((groupIds, ex) -> {

      if (ex == null) {
        promise.complete(Helper.fromConsumerGroupListings(groupIds));
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  @Override
  public void describeConsumerGroups(List<java.lang.String> groupIds, Handler<AsyncResult<Map<String, ConsumerGroupDescription>>> completionHandler) {
    describeConsumerGroups(groupIds).onComplete(completionHandler);
  }

  @Override
  public Future<Map<String, ConsumerGroupDescription>> describeConsumerGroups(List<String> groupIds) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<Map<String, ConsumerGroupDescription>> promise = ctx.promise();

    DescribeConsumerGroupsResult describeConsumerGroupsResult = this.adminClient.describeConsumerGroups(groupIds);
    describeConsumerGroupsResult.all().whenComplete((cg, ex) -> {
      if (ex == null) {
        Map<String, ConsumerGroupDescription> consumerGroups = new HashMap<>();

        for (Map.Entry<String, org.apache.kafka.clients.admin.ConsumerGroupDescription> cgDescriptionEntry: cg.entrySet()) {
          List<MemberDescription> members = new ArrayList<>();

          for (org.apache.kafka.clients.admin.MemberDescription memberDescription : cgDescriptionEntry.getValue().members()) {
            MemberDescription m = new MemberDescription();
            m.setConsumerId(memberDescription.consumerId())
              .setClientId(memberDescription.clientId())
              .setAssignment(Helper.from(memberDescription.assignment()))
              .setHost(memberDescription.host());

            members.add(m);
          }

          ConsumerGroupDescription consumerGroupDescription = new ConsumerGroupDescription();

          consumerGroupDescription.setGroupId(cgDescriptionEntry.getValue().groupId())
            .setCoordinator(Helper.from(cgDescriptionEntry.getValue().coordinator()))
            .setMembers(members)
            .setPartitionAssignor(cgDescriptionEntry.getValue().partitionAssignor())
            .setSimpleConsumerGroup(cgDescriptionEntry.getValue().isSimpleConsumerGroup())
            .setState(cgDescriptionEntry.getValue().state())
            .setAuthorizedOperations(cgDescriptionEntry.getValue().authorizedOperations());

          consumerGroups.put(cgDescriptionEntry.getKey(), consumerGroupDescription);
        }
        promise.complete(consumerGroups);
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  @Override
  public void describeConsumerGroups(List<String> groupIds, DescribeConsumerGroupsOptions options, Handler<AsyncResult<Map<String, ConsumerGroupDescription>>> completionHandler) {
    describeConsumerGroups(groupIds, options).onComplete(completionHandler);
  }

  @Override
  public Future<Map<String, ConsumerGroupDescription>> describeConsumerGroups(List<String> groupIds, DescribeConsumerGroupsOptions options) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<Map<String, ConsumerGroupDescription>> promise = ctx.promise();

    DescribeConsumerGroupsResult describeConsumerGroupsResult = this.adminClient.describeConsumerGroups(groupIds, Helper.to(options));
    describeConsumerGroupsResult.all().whenComplete((cg, ex) -> {
      if (ex == null) {
        Map<String, ConsumerGroupDescription> consumerGroups = new HashMap<>();

        for (Map.Entry<String, org.apache.kafka.clients.admin.ConsumerGroupDescription> cgDescriptionEntry: cg.entrySet()) {
          List<MemberDescription> members = new ArrayList<>();

          for (org.apache.kafka.clients.admin.MemberDescription memberDescription : cgDescriptionEntry.getValue().members()) {
            MemberDescription m = new MemberDescription();
            m.setConsumerId(memberDescription.consumerId())
              .setClientId(memberDescription.clientId())
              .setAssignment(Helper.from(memberDescription.assignment()))
              .setHost(memberDescription.host());

            members.add(m);
          }

          ConsumerGroupDescription consumerGroupDescription = new ConsumerGroupDescription();

          consumerGroupDescription.setGroupId(cgDescriptionEntry.getValue().groupId())
            .setCoordinator(Helper.from(cgDescriptionEntry.getValue().coordinator()))
            .setMembers(members)
            .setPartitionAssignor(cgDescriptionEntry.getValue().partitionAssignor())
            .setSimpleConsumerGroup(cgDescriptionEntry.getValue().isSimpleConsumerGroup())
            .setState(cgDescriptionEntry.getValue().state())
            .setAuthorizedOperations(cgDescriptionEntry.getValue().authorizedOperations());

          consumerGroups.put(cgDescriptionEntry.getKey(), consumerGroupDescription);
        }
        promise.complete(consumerGroups);
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  public void listConsumerGroupOffsets(String groupId, ListConsumerGroupOffsetsOptions options, Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>> completionHandler) {
    listConsumerGroupOffsets(groupId, options).onComplete(completionHandler);
  }

  public Future<Map<TopicPartition, OffsetAndMetadata>> listConsumerGroupOffsets(String groupId, ListConsumerGroupOffsetsOptions options) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<Map<TopicPartition, OffsetAndMetadata>> promise = ctx.promise();

    ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = this.adminClient.listConsumerGroupOffsets(groupId, Helper.to(options));
    listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().whenComplete((cgo, ex) -> {

      if (ex == null) {
        Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets = new HashMap<>();

        for (Map.Entry<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> cgoOffset : cgo.entrySet()) {
          consumerGroupOffsets.put(Helper.from(cgoOffset.getKey()), Helper.from(cgoOffset.getValue()));
        }
        promise.complete(consumerGroupOffsets);
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  @Override
  public void deleteConsumerGroups(List<String> groupIds, Handler<AsyncResult<Void>> completionHandler) {
    deleteConsumerGroups(groupIds).onComplete(completionHandler);
  }

  @Override
  public Future<Void> deleteConsumerGroups(List<String> groupIds) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<Void> promise = ctx.promise();

    DeleteConsumerGroupsResult deleteConsumerGroupsResult = this.adminClient.deleteConsumerGroups(groupIds);
    deleteConsumerGroupsResult.all().whenComplete((v, ex) -> {
      if (ex == null) {
        promise.complete();
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  @Override
  public void alterConsumerGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets, Handler<AsyncResult<Void>> completionHandler) {
    alterConsumerGroupOffsets(groupId, offsets).onComplete(completionHandler);
  }

  @Override
  public Future<Void> alterConsumerGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<Void> promise = ctx.promise();

    AlterConsumerGroupOffsetsResult alterConsumerGroupOffsetsResult = this.adminClient.alterConsumerGroupOffsets(groupId, Helper.to(offsets));
    alterConsumerGroupOffsetsResult.all().whenComplete((v, ex) -> {
      if (ex == null) {
        promise.complete();
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }


  @Override
  public void deleteConsumerGroupOffsets(String groupId, Set<TopicPartition> partitions, Handler<AsyncResult<Void>> completionHandler) {
    deleteConsumerGroupOffsets(groupId, partitions).onComplete(completionHandler);
  }

  @Override
  public Future<Void> deleteConsumerGroupOffsets(String groupId, Set<TopicPartition> partitions) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<Void> promise = ctx.promise();

    DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsetsResult = this.adminClient.deleteConsumerGroupOffsets(groupId, Helper.toTopicPartitionSet(partitions));
    deleteConsumerGroupOffsetsResult.all().whenComplete((v, ex) -> {
      if (ex == null) {
        promise.complete();
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  @Override
  public void describeCluster(Handler<AsyncResult<ClusterDescription>> completionHandler) {
    describeCluster().onComplete(completionHandler);
  }

  @Override
  public Future<ClusterDescription> describeCluster() {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<ClusterDescription> promise = ctx.promise();

    DescribeClusterResult describeClusterResult = this.adminClient.describeCluster();
    KafkaFuture.allOf(describeClusterResult.clusterId(), describeClusterResult.controller(), describeClusterResult.nodes(), describeClusterResult.authorizedOperations()).whenComplete((r, ex) -> {
      if (ex == null) {
        try {
          String clusterId = describeClusterResult.clusterId().get();
          org.apache.kafka.common.Node rcontroller = describeClusterResult.controller().get();
          Collection<org.apache.kafka.common.Node> rnodes = describeClusterResult.nodes().get();
          Set<AclOperation> authorizedOperations = describeClusterResult.authorizedOperations().get();
          Node controller = Helper.from(rcontroller);
          List<Node> nodes = new ArrayList<>();
          rnodes.forEach(rnode -> {
            nodes.add(Helper.from(rnode));
          });
          ClusterDescription clusterDescription = new ClusterDescription(clusterId, controller, nodes, authorizedOperations);
          promise.complete(clusterDescription);
        } catch (InterruptedException|ExecutionException e) {
          promise.fail(e);
        }
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  @Override
  public void describeCluster(DescribeClusterOptions options, Handler<AsyncResult<ClusterDescription>> completionHandler) {
    describeCluster(options).onComplete(completionHandler);
  }

  @Override
  public Future<ClusterDescription> describeCluster(DescribeClusterOptions options) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<ClusterDescription> promise = ctx.promise();

    DescribeClusterResult describeClusterResult = this.adminClient.describeCluster(Helper.to(options));
    KafkaFuture.allOf(describeClusterResult.clusterId(), describeClusterResult.controller(), describeClusterResult.nodes(), describeClusterResult.authorizedOperations()).whenComplete((r, ex) -> {
      if (ex == null) {
        try {
          String clusterId = describeClusterResult.clusterId().get();
          org.apache.kafka.common.Node rcontroller = describeClusterResult.controller().get();
          Collection<org.apache.kafka.common.Node> rnodes = describeClusterResult.nodes().get();
          Set<AclOperation> authorizedOperations = describeClusterResult.authorizedOperations().get();
          Node controller = Helper.from(rcontroller);
          List<Node> nodes = new ArrayList<>();
          rnodes.forEach(rnode -> {
            nodes.add(Helper.from(rnode));
          });
          ClusterDescription clusterDescription = new ClusterDescription(clusterId, controller, nodes, authorizedOperations);
          promise.complete(clusterDescription);
        } catch (InterruptedException|ExecutionException e) {
          promise.fail(e);
        }
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  public void listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets, Handler<AsyncResult<Map<TopicPartition, ListOffsetsResultInfo>>> completionHandler) {
    listOffsets(topicPartitionOffsets).onComplete(completionHandler);
  }

  @Override
  public Future<Map<TopicPartition, ListOffsetsResultInfo>> listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<Map<TopicPartition, ListOffsetsResultInfo>> promise = ctx.promise();

    ListOffsetsResult listOffsetsResult = this.adminClient.listOffsets(Helper.toTopicPartitionOffsets(topicPartitionOffsets));
    listOffsetsResult.all().whenComplete((o, ex) -> {
      if (ex == null) {
        Map<TopicPartition, ListOffsetsResultInfo> listOffsets = new HashMap<>();

        for (Map.Entry<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> oOffset : o.entrySet()) {
          listOffsets.put(Helper.from(oOffset.getKey()), Helper.from(oOffset.getValue()));
        }
        promise.complete(listOffsets);
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  public void describeAcls(AclBindingFilter aclBindingFilter, Handler<AsyncResult<List<AclBinding>>> completionHandler) {
    describeAcls(aclBindingFilter).onComplete(completionHandler);
  }

  public Future<List<AclBinding>> describeAcls(AclBindingFilter aclBindingFilter) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<List<AclBinding>> promise = ctx.promise();

    DescribeAclsResult describeAclsResult = this.adminClient.describeAcls(aclBindingFilter);
    describeAclsResult.values().whenComplete((o, ex) -> {
      if (ex == null) {
        List list = o.stream().map(entry -> new AclBinding(entry.pattern(), entry.entry())).collect(Collectors.toList());
        promise.complete(list);
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  public void createAcls(List<AclBinding> aclBindings, Handler<AsyncResult<List<AclBinding>>> completionHandler) {
    createAcls(aclBindings).onComplete(completionHandler);
  }

  public Future<List<AclBinding>> createAcls(List<AclBinding> aclBindings) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<List<AclBinding>> promise = ctx.promise();

    CreateAclsResult createAclsResult = this.adminClient.createAcls(aclBindings);
    createAclsResult.all().whenComplete((o, ex) -> {
      if (ex == null) {
        promise.complete();
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  public void deleteAcls(List<AclBindingFilter> aclBindingsFilters, Handler<AsyncResult<List<AclBinding>>> completionHandler) {
    deleteAcls(aclBindingsFilters).onComplete(completionHandler);
  }

  public Future<List<AclBinding>> deleteAcls(List<AclBindingFilter> aclBindingsFilters) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    Promise<List<AclBinding>> promise = ctx.promise();

    DeleteAclsResult deleteAclsResult = this.adminClient.deleteAcls(aclBindingsFilters);
    deleteAclsResult.all().whenComplete((o, ex) -> {
      if (ex == null) {
        promise.complete((List<AclBinding>) o);
      } else {
        promise.fail(ex);
      }
    });
    return promise.future();
  }

  @Override
  public Future<Void> close() {
    return close(0);
  }

  @Override
  public Future<Void> close(long timeout) {
    return vertx.executeBlocking(prom -> {
      if (timeout > 0) {
        adminClient.close(Duration.ofMillis(timeout));
      } else {
        adminClient.close();
      }
      prom.complete();
    });
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    close().onComplete(handler);
  }

  @Override
  public void close(long timeout, Handler<AsyncResult<Void>> handler) {
    close(timeout).onComplete(handler);
  }
}
