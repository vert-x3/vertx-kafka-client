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

package io.vertx.kafka.client.tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.vertx.kafka.admin.Config;
import io.vertx.kafka.admin.ConfigEntry;
import io.vertx.kafka.admin.ConsumerGroupDescription;
import io.vertx.kafka.admin.MemberAssignment;
import io.vertx.kafka.admin.MemberDescription;
import io.vertx.kafka.admin.NewTopic;
import io.vertx.kafka.admin.TopicDescription;
import io.vertx.kafka.client.common.ConfigResource;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.common.TopicPartitionInfo;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.admin.KafkaAdminClient;

public class AdminClientTest extends KafkaClusterTestBase {
  private Vertx vertx;
  private Properties config;

  private static Set<String> topics = new HashSet<>();

  static {
    topics.add("first-topic");
    topics.add("second-topic");
  }

  @Before
  public void beforeTest() {
      this.vertx = Vertx.vertx();
      this.config = new Properties();
      this.config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
  }

  @After
  public void afterTest(TestContext ctx) {
      this.vertx.close(ctx.asyncAssertSuccess());
  }

  @BeforeClass
  public static void setUp() throws IOException {
    KafkaClusterTestBase.setUp();
    kafkaCluster.createTopics(topics);
  }

  @Test
  public void testListTopics(TestContext ctx) {

    KafkaAdminClient adminClient = KafkaAdminClient.create(this.vertx, config);

    Async async = ctx.async();

    // timer because, Kafka cluster takes time to create topics
    vertx.setTimer(1000, t -> {
      adminClient.listTopics(ar -> {
        ctx.assertTrue(ar.succeeded());
        ctx.assertEquals(topics, ar.result());
        async.complete();
      });
    });

    async.await();
  }

  @Test
  public void testDescribeTopics(TestContext ctx) {

    KafkaAdminClient adminClient = KafkaAdminClient.create(this.vertx, config);

    Async async = ctx.async();

    // timer because, Kafka cluster takes time to create topics
    vertx.setTimer(1000, t -> {

      adminClient.describeTopics(Collections.singletonList("first-topic"), ar -> {
        ctx.assertTrue(ar.succeeded());

        TopicDescription topicDescription = ar.result().get("first-topic");
        ctx.assertNotNull(topicDescription);
        ctx.assertEquals("first-topic", topicDescription.getName());
        ctx.assertEquals(false, topicDescription.isInternal());
        ctx.assertEquals(1, topicDescription.getPartitions().size());

        TopicPartitionInfo topicPartitionInfo = topicDescription.getPartitions().get(0);
        ctx.assertEquals(0, topicPartitionInfo.getPartition());
        ctx.assertEquals(1, topicPartitionInfo.getLeader().getId());
        ctx.assertEquals(1, topicPartitionInfo.getReplicas().size());
        ctx.assertEquals(1, topicPartitionInfo.getReplicas().get(0).getId());
        ctx.assertEquals(1, topicPartitionInfo.getIsr().size());
        ctx.assertEquals(1, topicPartitionInfo.getIsr().get(0).getId());

        async.complete();
      });
    });

    async.await();
  }

  @Test
  public void testCreateTopic(TestContext ctx) {

    KafkaAdminClient adminClient = KafkaAdminClient.create(this.vertx, config);

    Async async = ctx.async();

    adminClient.createTopics(Collections.singletonList(new NewTopic("testCreateTopic", 1, (short)1)), ar -> {
      ctx.assertTrue(ar.succeeded());

      adminClient.describeTopics(Collections.singletonList("testCreateTopic"), ar1 -> {
        ctx.assertTrue(ar1.succeeded());
        TopicDescription topicDescription = ar1.result().get("testCreateTopic");

        ctx.assertEquals("testCreateTopic", topicDescription.getName());
        ctx.assertEquals(1, topicDescription.getPartitions().size());
        ctx.assertEquals(1, topicDescription.getPartitions().get(0).getReplicas().size());

        adminClient.deleteTopics(Collections.singletonList("testCreateTopic"), ar2 -> {
          ctx.assertTrue(ar2.succeeded());
          async.complete();
        });

      });

    });

    async.await();
  }

  @Test
  public void testCreateTopicWithConfigs(TestContext ctx) {

    KafkaAdminClient adminClient = KafkaAdminClient.create(this.vertx, config);

    Async async = ctx.async();

    NewTopic newTopic = new NewTopic("testCreateTopicWithConfigs", 1, (short)1);
    newTopic.setConfig(Collections.singletonMap("segment.bytes", "1000"));

    adminClient.createTopics(Collections.singletonList(newTopic), ar -> {
      ctx.assertTrue(ar.succeeded());

      adminClient.describeTopics(Collections.singletonList("testCreateTopicWithConfigs"), ar1 -> {
        ctx.assertTrue(ar1.succeeded());
        TopicDescription topicDescription = ar1.result().get("testCreateTopicWithConfigs");

        ctx.assertEquals("testCreateTopicWithConfigs", topicDescription.getName());
        ctx.assertEquals(1, topicDescription.getPartitions().size());
        ctx.assertEquals(1, topicDescription.getPartitions().get(0).getReplicas().size());

        ConfigResource configResource =
          new ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC, "testCreateTopicWithConfigs");

        adminClient.describeConfigs(Collections.singletonList(configResource), ar2 -> {
            ctx.assertTrue(ar2.succeeded());

            Optional<ConfigEntry> configEntry = ar2.result().get(configResource).getEntries().stream()
              .filter(e -> e.getName().equals("segment.bytes"))
              .findFirst();
            ctx.assertTrue(configEntry.isPresent());
            ctx.assertEquals("1000", configEntry.get().getValue());

            adminClient.deleteTopics(Collections.singletonList("testCreateTopicWithConfigs"), ar3 -> {
              ctx.assertTrue(ar3.succeeded());
              async.complete();
            });
        });

      });

    });

    async.await();
  }


  @Test
  public void testDeleteTopic(TestContext ctx) {

    KafkaAdminClient adminClient = KafkaAdminClient.create(this.vertx, config);

    kafkaCluster.createTopic("topicToDelete", 1, 1);

    Async async = ctx.async();

    // timer because, Kafka cluster takes time to create topics
    vertx.setTimer(1000, t -> {

      adminClient.listTopics(ar -> {
        ctx.assertTrue(ar.succeeded());
        ctx.assertTrue(ar.result().contains("topicToDelete"));

        adminClient.deleteTopics(Collections.singletonList("topicToDelete"), ar1 -> {
          ctx.assertTrue(ar1.succeeded());
          async.complete();
        });

      });

      async.await();
    });
  }

  @Test
  public void testDescribeConfigs(TestContext ctx) {

    KafkaAdminClient adminClient = KafkaAdminClient.create(this.vertx, config);

    Async async = ctx.async();

    adminClient.describeConfigs(Collections.singletonList(
      new ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC, "first-topic")), ar -> {

      ctx.assertTrue(ar.succeeded());
      ctx.assertFalse(ar.result().isEmpty());

      async.complete();
    });

    async.await();
  }

  @Test
  public void testAlterConfigs(TestContext ctx) {

    KafkaAdminClient adminClient = KafkaAdminClient.create(this.vertx, config);

    Async async = ctx.async();

    ConfigResource resource = new ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC, "first-topic");
    // create a entry for updating the retention.ms value on the topic
    ConfigEntry retentionEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "51000");
    Map<ConfigResource, Config> updateConfig = new HashMap<>();
    updateConfig.put(resource, new Config(Collections.singletonList(retentionEntry)));

    adminClient.alterConfigs(updateConfig, ar -> {

      ctx.assertTrue(ar.succeeded());

      adminClient.describeConfigs(Collections.singletonList(resource), ar1 -> {

        ctx.assertTrue(ar1.succeeded());

        Map<ConfigResource, Config> describeConfig = ar1.result();
        ConfigEntry describeRetentionEntry =
          describeConfig.get(resource)
            .getEntries()
            .stream()
            .filter(entry -> entry.getName().equals(TopicConfig.RETENTION_MS_CONFIG))
            .collect(Collectors.toList())
            .get(0);

        ctx.assertEquals("51000", describeRetentionEntry.getValue());

        async.complete();
      });
    });

    async.await();
  }

  @Test
  public void testListConsumerGroups(TestContext ctx) {

    KafkaAdminClient adminClient = KafkaAdminClient.create(this.vertx, config);

    Async async = ctx.async();

    //kafkaCluster.useTo().consumeStrings(() -> true, null, Collections.singletonList("first-topic"), c -> { });

    kafkaCluster.useTo().consume("groupId", "clientId", OffsetResetStrategy.EARLIEST,
      new StringDeserializer(), new StringDeserializer(), () -> true, null, null,
      Collections.singleton("first-topic"), c -> { });

    // timer because, Kafka cluster takes time to start consumer
    vertx.setTimer(1000, t -> {

      adminClient.listConsumerGroups(ar -> {
        ctx.assertTrue(ar.succeeded());
        ctx.assertEquals(1, ar.result().size());
        ctx.assertEquals("groupId", ar.result().get(0).getGroupId());
        async.complete();
      });

    });

    async.await();
  }

  @Test
  public void testDescribeConsumerGroups(TestContext ctx) {

    KafkaAdminClient adminClient = KafkaAdminClient.create(this.vertx, config);

    Async async = ctx.async();

    //kafkaCluster.useTo().consumeStrings(() -> true, null, Collections.singletonList("first-topic"), c -> { });

    kafkaCluster.useTo().consume("groupId", "clientId", OffsetResetStrategy.EARLIEST,
      new StringDeserializer(), new StringDeserializer(), () -> true, null, null,
      Collections.singleton("first-topic"), c -> { });

    // timer because, Kafka cluster takes time to start consumer
    vertx.setTimer(1000, t -> {

      adminClient.describeConsumerGroups(Collections.singletonList("groupId"), ar -> {
        ctx.assertTrue(ar.succeeded());

        ConsumerGroupDescription consumerGroupDescription = ar.result().get("groupId");
        ctx.assertNotNull(consumerGroupDescription);
        ctx.assertEquals("groupId", consumerGroupDescription.getGroupId());
        ctx.assertEquals(1, consumerGroupDescription.getMembers().size());

        MemberDescription memberDescription = consumerGroupDescription.getMembers().get(0);
        ctx.assertEquals("clientId", memberDescription.getClientId());
        ctx.assertEquals(1, memberDescription.getAssignment().getTopicPartitions().size());

        Iterator<TopicPartition> iterator = memberDescription.getAssignment().getTopicPartitions().iterator();
        ctx.assertTrue(iterator.hasNext());
        ctx.assertEquals("first-topic", iterator.next().getTopic());

        async.complete();
      });

    });

    async.await();
  }

  @Test
  public void testConsumersOnTopics(TestContext ctx) {

    List<ConsumerGroupDescription> cGroups = new ArrayList<>();

    List<MemberDescription> members1 = new ArrayList<>();

    MemberDescription m1 = new MemberDescription();
    MemberAssignment ma1 = new MemberAssignment();
    Set<TopicPartition> setTp1 = new HashSet<>();
    setTp1.add(new TopicPartition("my-topic", 0));
    setTp1.add(new TopicPartition("my-topic", 1));
    setTp1.add(new TopicPartition("your-topic", 0));
    ma1.setTopicPartitions(setTp1);
    m1.setAssignment(ma1);

    MemberDescription m2 = new MemberDescription();
    MemberAssignment ma2 = new MemberAssignment();
    Set<TopicPartition> setTp2 = new HashSet<>();
    setTp2.add(new TopicPartition("my-topic", 2));
    setTp2.add(new TopicPartition("my-topic", 3));
    setTp2.add(new TopicPartition("his-topic", 0));
    ma2.setTopicPartitions(setTp2);
    m2.setAssignment(ma2);

    members1.add(m1);
    members1.add(m2);

    List<MemberDescription> members2 = new ArrayList<>();

    MemberDescription m3 = new MemberDescription();
    MemberAssignment ma3 = new MemberAssignment();
    Set<TopicPartition> setTp3 = new HashSet<>();
    setTp3.add(new TopicPartition("my-topic", 0));
    setTp3.add(new TopicPartition("my-topic", 1));
    setTp3.add(new TopicPartition("my-topic", 2));
    setTp3.add(new TopicPartition("my-topic", 3));
    setTp3.add(new TopicPartition("his-topic", 0));
    setTp3.add(new TopicPartition("your-topic", 0));
    ma3.setTopicPartitions(setTp3);
    m3.setAssignment(ma3);

    members2.add(m3);

    cGroups.add(new ConsumerGroupDescription().setGroupId("groupid-1").setMembers(members1));
    cGroups.add(new ConsumerGroupDescription().setGroupId("groupid-2").setMembers(members2));

    Map<String, Integer> consumers = new HashMap<>();

    for (ConsumerGroupDescription cgd : cGroups) {

      for (MemberDescription m : cgd.getMembers()) {

        List<String> topics = m.getAssignment().getTopicPartitions().stream()
          .filter(distinctByKey(tp -> tp.getTopic()))
          .map(tp -> tp.getTopic())
          .collect(Collectors.toList());

        for (String topic : topics) {

          consumers.merge(topic, 1, Integer::sum);
        }
      }

    }

    ctx.assertEquals(3, consumers.get("my-topic"));
    ctx.assertEquals(2, consumers.get("your-topic"));
    ctx.assertEquals(2, consumers.get("his-topic"));
  }

  private static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
    Set<Object> seen = ConcurrentHashMap.newKeySet();
    return t -> seen.add(keyExtractor.apply(t));
  }
}
