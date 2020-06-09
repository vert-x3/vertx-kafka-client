/*
 * Copyright 2016 Red Hat Inc.
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

package examples;

import io.vertx.core.Vertx;
import io.vertx.docgen.Source;
import io.vertx.kafka.admin.Config;
import io.vertx.kafka.admin.ConfigEntry;
import io.vertx.kafka.admin.ConsumerGroupDescription;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.MemberDescription;
import io.vertx.kafka.admin.NewTopic;
import io.vertx.kafka.admin.TopicDescription;
import io.vertx.kafka.client.common.ConfigResource;
import io.vertx.kafka.client.common.TopicPartitionInfo;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.TopicConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Source
public class KafkaAdminClientExamples {

  /**
   * Example about Kafka Admin Client creation
   * @param vertx Vert.x instance
   */
  public void exampleCreateAdminClient(Vertx vertx) {
    // creating the admin client using properties config
    Properties config = new Properties();
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    KafkaAdminClient adminClient = KafkaAdminClient.create(vertx, config);
  }

  /**
   * Example about listing topics
   * @param adminClient Kafka admin client instance
   */
  public void exampleListTopics(KafkaAdminClient adminClient) {
    adminClient.listTopics().onSuccess(topics ->
        System.out.println("Topics= " + topics)
    );
  }

  /**
   * Example about describing topics
   * @param adminClient Kafka admin client instance
   */
  public void exampleDescribeTopics(KafkaAdminClient adminClient) {
    adminClient.describeTopics(Collections.singletonList("my-topic")).onSuccess(topics -> {
      TopicDescription topicDescription = topics.get("first-topic");

      System.out.println("Topic name=" + topicDescription.getName() +
          " isInternal= " + topicDescription.isInternal() +
          " partitions= " + topicDescription.getPartitions().size());

      for (TopicPartitionInfo topicPartitionInfo : topicDescription.getPartitions()) {
        System.out.println("Partition id= " + topicPartitionInfo.getPartition() +
          " leaderId= " + topicPartitionInfo.getLeader().getId() +
          " replicas= " + topicPartitionInfo.getReplicas() +
          " isr= " + topicPartitionInfo.getIsr());
      }
    });
  }

  /**
   * Example about deleting topics
   * @param adminClient Kafka admin client instance
   */
  public void exampleDeleteTopics(KafkaAdminClient adminClient) {
    adminClient.deleteTopics(Collections.singletonList("topicToDelete"))
      .onSuccess(v -> {
        // topics deleted successfully
      })
      .onFailure(cause -> {
        // something went wrong when removing the topics
      });
  }

  /**
   * Example about creating topics
   * @param adminClient Kafka admin client instance
   */
  public void exampleCreateTopics(KafkaAdminClient adminClient) {
    adminClient.createTopics(Collections.singletonList(new NewTopic("testCreateTopic", 1, (short)1)))
      .onSuccess(v -> {
        // topics created successfully
      })
      .onFailure(cause -> {
        // something went wrong when creating the topics
      });
  }

  /**
   * Example about describing resources configuration like topic or broker
   * @param adminClient Kafka admin client instance
   */
  public void exampleDescribeConfigs(KafkaAdminClient adminClient) {
    // describe configuration for a topic
    adminClient.describeConfigs(Collections.singletonList(
      new ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC, "my-topic"))).onSuccess(configs -> {
      // check the configurations
    });
  }

  /**
   * Example about altering resources configuration like topic or broker
   * @param adminClient Kafka admin client instance
   */
  public void exampleAlterConfigs(KafkaAdminClient adminClient) {
    ConfigResource resource = new ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC, "my-topic");
    // create a entry for updating the retention.ms value on the topic
    ConfigEntry retentionEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "51000");
    Map<ConfigResource, Config> updateConfig = new HashMap<>();
    updateConfig.put(resource, new Config(Collections.singletonList(retentionEntry)));
    adminClient.alterConfigs(updateConfig)
      .onSuccess(v -> {
        // configuration altered successfully
      })
      .onFailure(cause -> {
        // something went wrong when altering configs
      });
  }

  /**
   * Example about listing consumer groups
   * @param adminClient Kafka admin client instance
   */
  public void exampleListConsumerGroups(KafkaAdminClient adminClient) {
    adminClient.listConsumerGroups().onSuccess(consumerGroups ->
      System.out.println("ConsumerGroups= " + consumerGroups)
    );
  }

  /**
   * Example about describing consumer groups
   * @param adminClient Kafka admin client instance
   */
  public void exampleDescribeConsumerGroups(KafkaAdminClient adminClient) {
    adminClient.describeConsumerGroups(Collections.singletonList("my-group")).onSuccess(consumerGroups -> {
      ConsumerGroupDescription consumerGroupDescription = consumerGroups.get("my-group");

      System.out.println("Group id=" + consumerGroupDescription.getGroupId() +
        " state= " + consumerGroupDescription.getState() +
        " coordinator host= " + consumerGroupDescription.getCoordinator().getHost());

      for (MemberDescription memberDescription : consumerGroupDescription.getMembers()) {
        System.out.println("client id= " + memberDescription.getClientId() +
          " topic partitions= " + memberDescription.getAssignment().getTopicPartitions());
      }
    });
  }
}
