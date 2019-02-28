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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.kafka.client.common.ConfigResource;
import org.apache.kafka.clients.admin.AdminClient;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.admin.impl.KafkaAdminClientImpl;

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
  void listTopics(Handler<AsyncResult<Set<String>>> completionHandler);

  /**
   * Describe some topics in the cluster, with the default options.
   *
   * @param topicNames the names of the topics to describe
   * @param completionHandler handler called on operation completed with the topics descriptions
   */
  @GenIgnore
  void describeTopics(List<String> topicNames, Handler<AsyncResult<Map<String, TopicDescription>>> completionHandler);

  /**
   * Creates a batch of new Kafka topics
   *
   * @param topics topics to create
   * @param completionHandler handler called on operation completed
   */
  void createTopics(List<NewTopic> topics, Handler<AsyncResult<Void>> completionHandler);

  /**
   * Deletes a batch of Kafka topics
   *
   * @param topicNames the names of the topics to delete
   * @param completionHandler handler called on operation completed
   */
  void deleteTopics(List<String> topicNames, Handler<AsyncResult<Void>> completionHandler);

  /**
   * Get the configuration for the specified resources with the default options
   *
   * @param configResources the resources (topic and broker resource types are currently supported)
   * @param completionHandler handler called on operation completed with the configurations
   */
  @GenIgnore
  void describeConfigs(List<ConfigResource> configResources, Handler<AsyncResult<Map<ConfigResource, Config>>> completionHandler);

  /**
   * Update the configuration for the specified resources with the default options
   *
   * @param configs The resources with their configs (topic is the only resource type with configs that can be updated currently)
   * @param completionHandler handler called on operation completed
   */
  @GenIgnore
  void alterConfigs(Map<ConfigResource,Config> configs, Handler<AsyncResult<Void>> completionHandler);

  /**
   * Get the the consumer groups available in the cluster with the default options
   *
   * @param completionHandler handler called on operation completed with the consumer groups ids
   */
  void listConsumerGroups(Handler<AsyncResult<List<ConsumerGroupListing>>> completionHandler);

  /**
   * Describe some group ids in the cluster, with the default options
   *
   * @param groupIds the ids of the groups to describe
   * @param completionHandler handler called on operation completed with the consumer groups descriptions
   */
  void describeConsumerGroups(List<java.lang.String> groupIds, Handler<AsyncResult<Map<String, ConsumerGroupDescription>>> completionHandler);
}
