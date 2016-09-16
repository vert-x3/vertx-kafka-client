package io.vertx.kafka.client.tests;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.runner.RunWith;

import java.io.File;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
@RunWith(VertxUnitRunner.class)
public class KafkaClusterTestBase extends KafkaTestBase {

  private File dataDir;
  private KafkaCluster kafkaCluster;

  protected KafkaCluster kafkaCluster() {
    if (kafkaCluster != null) {
      throw new IllegalStateException();
    }
    dataDir = Testing.Files.createTestingDirectory("cluster");
    kafkaCluster = new KafkaCluster().usingDirectory(dataDir).withPorts(2181, 9092);
    return kafkaCluster;
  }

  @After
  public void afterTest(TestContext ctx) {
    if (kafkaCluster != null) {
      kafkaCluster.shutdown();
      kafkaCluster = null;
      dataDir.delete();
    }
  }
}
