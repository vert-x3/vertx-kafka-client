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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.vertx.core.Promise;
import io.vertx.kafka.admin.Config;
import io.vertx.kafka.admin.ConsumerGroupDescription;
import io.vertx.kafka.admin.ConsumerGroupListing;
import io.vertx.kafka.admin.MemberDescription;
import io.vertx.kafka.admin.NewTopic;
import io.vertx.kafka.admin.TopicDescription;
import io.vertx.kafka.client.common.ConfigResource;
import io.vertx.kafka.client.common.TopicPartitionInfo;
import io.vertx.kafka.client.common.impl.Helper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.admin.KafkaAdminClient;

public class KafkaAdminClientImpl implements KafkaAdminClient {

  private Vertx vertx;
  private AdminClient adminClient;

  public KafkaAdminClientImpl(Vertx vertx, AdminClient adminClient) {
      this.vertx = vertx;
      this.adminClient = adminClient;
  }

  @Override
  public void describeTopics(List<String> topicNames, Handler<AsyncResult<Map<String, TopicDescription>>> completionHandler) {

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
            .setPartitions(partitions);

          topics.put(topicDescriptionEntry.getKey(), topicDescription);
        }

        completionHandler.handle(Future.succeededFuture(topics));
      } else {
        completionHandler.handle(Future.failedFuture(ex));
      }
    });
  }

  @Override
  public Future<Map<String, TopicDescription>> describeTopics(List<String> topicNames) {
    Promise<Map<String, TopicDescription>> promise = Promise.promise();
    describeTopics(topicNames, promise);
    return promise.future();
  }

  @Override
  public void listTopics(Handler<AsyncResult<Set<String>>> completionHandler) {

    ListTopicsResult listTopicsResult = this.adminClient.listTopics();
    listTopicsResult.names().whenComplete((topics, ex) -> {

      if (ex == null) {
          completionHandler.handle(Future.succeededFuture(topics));
      } else {
          completionHandler.handle(Future.failedFuture(ex));
      }
    });
  }

  @Override
  public Future<Set<String>> listTopics() {
    Promise<Set<String>> promise = Promise.promise();
    listTopics(promise);
    return promise.future();
  }

  @Override
  public void createTopics(List<NewTopic> topics, Handler<AsyncResult<Void>> completionHandler) {

    CreateTopicsResult createTopicsResult = this.adminClient.createTopics(Helper.toNewTopicList(topics));
    createTopicsResult.all().whenComplete((v, ex) -> {

      if (ex == null) {
        completionHandler.handle(Future.succeededFuture());
      } else {
        completionHandler.handle(Future.failedFuture(ex));
      }
    });
  }

  @Override
  public Future<Void> createTopics(List<NewTopic> topics) {
    Promise<Void> promise = Promise.promise();
    createTopics(topics, promise);
    return promise.future();
  }

  @Override
  public void deleteTopics(List<String> topicNames, Handler<AsyncResult<Void>> completionHandler) {

    DeleteTopicsResult deleteTopicsResult = this.adminClient.deleteTopics(topicNames);
    deleteTopicsResult.all().whenComplete((v, ex) -> {

      if (ex == null) {
        completionHandler.handle(Future.succeededFuture());
      } else {
        completionHandler.handle(Future.failedFuture(ex));
      }
    });
  }

  @Override
  public Future<Void> deleteTopics(List<String> topicNames) {
    Promise<Void> promise = Promise.promise();
    deleteTopics(topicNames, promise);
    return promise.future();
  }

  @Override
  public void describeConfigs(List<ConfigResource> configResources, Handler<AsyncResult<Map<ConfigResource, Config>>> completionHandler) {

    DescribeConfigsResult describeConfigsResult = this.adminClient.describeConfigs(Helper.toConfigResourceList(configResources));
    describeConfigsResult.all().whenComplete((m, ex) -> {

      if (ex == null) {

        Map<ConfigResource, Config> configs = new HashMap<>();

        for (Map.Entry<org.apache.kafka.common.config.ConfigResource, org.apache.kafka.clients.admin.Config> configEntry : m.entrySet()) {

          ConfigResource configResource = Helper.from(configEntry.getKey());
          Config config = Helper.from(configEntry.getValue());

          configs.put(configResource, config);
        }

        completionHandler.handle(Future.succeededFuture(configs));
      } else {
        completionHandler.handle(Future.failedFuture(ex));
      }
    });
  }

  @Override
  public Future<Map<ConfigResource, Config>> describeConfigs(List<ConfigResource> configResources) {
    Promise<Map<ConfigResource, Config>> promise = Promise.promise();
    describeConfigs(configResources, promise);
    return promise.future();
  }

  @Override
  public void alterConfigs(Map<ConfigResource,Config> configs, Handler<AsyncResult<Void>> completionHandler) {

    AlterConfigsResult alterConfigsResult = this.adminClient.alterConfigs(Helper.toConfigMaps(configs));
    alterConfigsResult.all().whenComplete((v, ex) -> {

      if (ex == null) {
        completionHandler.handle(Future.succeededFuture());
      } else {
        completionHandler.handle(Future.failedFuture(ex));
      }
    });
  }

  @Override
  public Future<Void> alterConfigs(Map<ConfigResource, Config> configs) {
    Promise<Void> promise = Promise.promise();
    alterConfigs(configs, promise);
    return promise.future();
  }

  @Override
  public void listConsumerGroups(Handler<AsyncResult<List<ConsumerGroupListing>>> completionHandler) {

    ListConsumerGroupsResult listConsumerGroupsResult = this.adminClient.listConsumerGroups();
    listConsumerGroupsResult.all().whenComplete((groupIds, ex) -> {

      if (ex == null) {
        completionHandler.handle(Future.succeededFuture(Helper.fromConsumerGroupListings(groupIds)));
      } else {
        completionHandler.handle(Future.failedFuture(ex));
      }
    });
  }

  @Override
  public Future<List<ConsumerGroupListing>> listConsumerGroups() {
    Promise<List<ConsumerGroupListing>> promise = Promise.promise();
    listConsumerGroups(promise);
    return promise.future();
  }

  @Override
  public void describeConsumerGroups(List<java.lang.String> groupIds, Handler<AsyncResult<Map<String, ConsumerGroupDescription>>> completionHandler) {

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
            .setState(cgDescriptionEntry.getValue().state());

          consumerGroups.put(cgDescriptionEntry.getKey(), consumerGroupDescription);
        }

        completionHandler.handle(Future.succeededFuture(consumerGroups));
      } else {
        completionHandler.handle(Future.failedFuture(ex));
      }
    });
  }

  @Override
  public Future<Map<String, ConsumerGroupDescription>> describeConsumerGroups(List<String> groupIds) {
    Promise<Map<String, ConsumerGroupDescription>> promise = Promise.promise();
    describeConsumerGroups(groupIds, promise);
    return promise.future();
  }
}
