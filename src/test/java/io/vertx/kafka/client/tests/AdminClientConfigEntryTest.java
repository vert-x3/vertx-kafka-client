/*
 * Copyright 2025 Red Hat Inc.
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
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.admin.ConfigEntry;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.NewTopic;
import io.vertx.kafka.client.common.ConfigResource;

public class AdminClientConfigEntryTest extends KafkaClusterTestBase {
    private static final String MIN_INSYNC_REPLICAS = "min.insync.replicas"; 
    private Vertx vertx;
    private Properties config;

    @Before
    public void beforeTest() {
        this.vertx = Vertx.vertx();
        this.config = new Properties();
        this.config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    }

    @After
    public void afterTest(TestContext ctx) {
        this.vertx.close().onComplete(ctx.asyncAssertSuccess());
    }

    @BeforeClass
    public static void setUp() throws IOException {
        kafkaCluster = kafkaCluster(true).deleteDataPriorToStartup(true).addBrokers(2).startup();
    }

    @Test
    public void testPropertiesOfEntryNotConfiguredExplicitly(TestContext ctx) {
        KafkaAdminClient adminClient = KafkaAdminClient.create(this.vertx, config);
        
        String topicName = "topic-default-min-isr";
        NewTopic topic = new NewTopic(topicName, 1, (short)1);

        adminClient.createTopics(Collections.singletonList(topic)).onComplete(ctx.asyncAssertSuccess(v -> {
    
            ConfigResource topicResource = new ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC, topicName);
            adminClient.describeConfigs(Collections.singletonList(topicResource)).onComplete(ctx.asyncAssertSuccess(desc -> {
                
                ConfigEntry minISREntry = desc.get(topicResource)
                .getEntries()
                .stream()
                .filter(entry -> MIN_INSYNC_REPLICAS.equals(entry.getName()))
                .findFirst()
                .get();

                ctx.assertTrue(minISREntry.isDefault());
                ctx.assertEquals(minISREntry.getSource(), org.apache.kafka.clients.admin.ConfigEntry.ConfigSource.DEFAULT_CONFIG);

                adminClient.deleteTopics(Collections.singletonList(topicName)).onComplete(ctx.asyncAssertSuccess(r -> {
                    adminClient.close();
                }));
            }));
        }));
    }

    @Test
    public void testPropertiesOfEntryConfiguredExplicitly(TestContext ctx) {
        KafkaAdminClient adminClient = KafkaAdminClient.create(this.vertx, config);
        
        String topicName = "topic-custom-min-isr";
        NewTopic topic = new NewTopic(topicName, 1, (short)1);
        topic.setConfig(Collections.singletonMap(MIN_INSYNC_REPLICAS, "1"));

        adminClient.createTopics(Collections.singletonList(topic)).onComplete(ctx.asyncAssertSuccess(v -> {
    
            ConfigResource topicResource = new ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC, topicName);
            adminClient.describeConfigs(Collections.singletonList(topicResource)).onComplete(ctx.asyncAssertSuccess(desc -> {
                
                ConfigEntry minISREntry = desc.get(topicResource)
                .getEntries()
                .stream()
                .filter(entry -> MIN_INSYNC_REPLICAS.equals(entry.getName()))
                .findFirst()
                .get();

                ctx.assertFalse(minISREntry.isDefault());
                ctx.assertEquals(minISREntry.getSource(), org.apache.kafka.clients.admin.ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);

                adminClient.deleteTopics(Collections.singletonList(topicName)).onComplete(ctx.asyncAssertSuccess(r -> {
                    adminClient.close();
                }));
            }));
        }));
    }


}
