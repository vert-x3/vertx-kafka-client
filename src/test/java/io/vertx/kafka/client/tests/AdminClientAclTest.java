/*
 * Copyright 2021 Red Hat Inc.
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

import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class AdminClientAclTest extends KafkaClusterTestBase {
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
        this.vertx.close(ctx.asyncAssertSuccess());
    }

    @BeforeClass
    public static void setUp() throws IOException {
        kafkaCluster = kafkaCluster(true).deleteDataPriorToStartup(true).addBrokers(2).startup();
    }

    @Test
    public void testDescribeEmptyAcl(TestContext ctx) {
        KafkaAdminClient adminClient = KafkaAdminClient.create(this.vertx, config);
        ResourcePatternFilter rpf = new ResourcePatternFilter(ResourceType.TOPIC, "test-acl-topic", PatternType.LITERAL);
        AccessControlEntryFilter acef = new AccessControlEntryFilter("User:*", "localhost:9092", AclOperation.DESCRIBE, AclPermissionType.ALLOW);
        adminClient.describeAcls(new AclBindingFilter(rpf, acef), ctx.asyncAssertSuccess(list -> {
            ctx.assertTrue(list.isEmpty());
            adminClient.close();
        }));
    }

    @Test
    public void testCreateDescribeDeleteDescribeAcl(TestContext ctx) {
        String topicName = "test-topic";
        String host = "localhost:9092";
        String principal = "User:ANONYMOUS";
        KafkaAdminClient adminClient = KafkaAdminClient.create(this.vertx, config);
        ResourcePattern resourcePattern = new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL);
        AccessControlEntry ace = new AccessControlEntry(principal, host, AclOperation.ALL, AclPermissionType.ALLOW);
        AclBinding aclBinding = new AclBinding(resourcePattern, ace);

        ResourcePatternFilter rpf = new ResourcePatternFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL);
        AccessControlEntryFilter acef = new AccessControlEntryFilter(principal, host, AclOperation.ALL, AclPermissionType.ALLOW);

        AclBindingFilter abf = new AclBindingFilter(rpf, acef);
        adminClient.createAcls(Collections.singletonList(aclBinding)).onComplete(ctx.asyncAssertSuccess(i ->
            adminClient.describeAcls(abf, ctx.asyncAssertSuccess(list -> {
                ctx.assertFalse(list.isEmpty());
                ctx.assertTrue(list.get(0).entry().host().equals(host));
                ctx.assertTrue(list.get(0).entry().principal().equals(principal));
                ctx.assertTrue(list.get(0).entry().operation().equals(AclOperation.ALL));
                ctx.assertTrue(list.get(0).entry().permissionType().equals(AclPermissionType.ALLOW));
                ctx.assertTrue(list.get(0).pattern().name().equals(topicName));
                ctx.assertTrue(list.get(0).pattern().patternType().equals(PatternType.LITERAL));
                ctx.assertTrue(list.get(0).pattern().resourceType().equals(ResourceType.TOPIC));
                adminClient.deleteAcls(Collections.singletonList(abf)).onComplete(ctx.asyncAssertSuccess(deleted -> {
                    ctx.assertFalse(deleted.isEmpty());
                    ctx.assertTrue(deleted.get(0).entry().host().equals(host));
                    ctx.assertTrue(deleted.get(0).entry().principal().equals(principal));
                    ctx.assertTrue(deleted.get(0).entry().operation().equals(AclOperation.ALL));
                    ctx.assertTrue(deleted.get(0).entry().permissionType().equals(AclPermissionType.ALLOW));
                    ctx.assertTrue(deleted.get(0).pattern().name().equals(topicName));
                    ctx.assertTrue(deleted.get(0).pattern().patternType().equals(PatternType.LITERAL));
                    ctx.assertTrue(deleted.get(0).pattern().resourceType().equals(ResourceType.TOPIC));
                    adminClient.describeAcls(abf).onComplete(ctx.asyncAssertSuccess(list2 -> {
                        ctx.assertTrue(list2.isEmpty());
                        adminClient.close();
                    }));
                }));
            }))));
    }
}
