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

package io.vertx.kafka.client.tests;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Base class for tests providing a Kafka cluster
 */
@RunWith(VertxUnitRunner.class)
public class KafkaClusterTestBase extends KafkaTestBase {

  protected static File dataDir;
  protected static KafkaCluster kafkaCluster;

  public static KafkaCluster kafkaCluster(boolean acl) {
    if (kafkaCluster != null) {
      throw new IllegalStateException();
    }
    dataDir = Testing.Files.createTestingDirectory("cluster");
    Properties kafkaProps = new Properties();
    if (acl) {
      kafkaProps.put("authorizer.class.name", "kafka.security.authorizer.AclAuthorizer");
      kafkaProps.put("super.users", "User:ANONYMOUS");
    }
    kafkaCluster = new KafkaCluster().usingDirectory(dataDir).withPorts(2181, 9092).withKafkaConfiguration(kafkaProps);
    return kafkaCluster;
  }

  @BeforeClass
  public static void setUp() throws IOException {
    kafkaCluster = kafkaCluster(false).deleteDataPriorToStartup(true).addBrokers(1).startup();
  }


  @AfterClass
  public static void tearDown() {
    if (kafkaCluster != null) {
      kafkaCluster.shutdown();
      kafkaCluster = null;
      boolean delete = dataDir.delete();
      // If files are still locked and a test fails: delete on exit to allow subsequent test execution
      if(!delete) {
        dataDir.deleteOnExit();
      }
    }
  }
}
