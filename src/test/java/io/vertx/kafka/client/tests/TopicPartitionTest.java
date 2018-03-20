package io.vertx.kafka.client.tests;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.common.TopicPartition;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class TopicPartitionTest {

  @Test
  public void testEquality(final TestContext context) {
    final TopicPartition t1 = new TopicPartition("topic1", 0);
    final TopicPartition t2 = new TopicPartition("topic1", 0);
    final TopicPartition t3 = new TopicPartition(null, 0);
    final TopicPartition t4 = new TopicPartition(null, 0);

    context.assertEquals(t1, t1);
    context.assertEquals(t1.hashCode(), t1.hashCode());

    context.assertEquals(t1, t2);
    context.assertEquals(t1.hashCode(), t2.hashCode());

    context.assertEquals(t3, t4);
    context.assertEquals(t3.hashCode(), t4.hashCode());
  }

  @Test
  public void testUnequality(final TestContext context) {
    final TopicPartition t1 = new TopicPartition("topic1", 0);
    final TopicPartition t2 = new TopicPartition("topic1", 1);
    final TopicPartition t3 = new TopicPartition("topic2", 0);
    final TopicPartition t4 = new TopicPartition("topic2", 1);
    final JsonObject t5 = new JsonObject();

    context.assertNotEquals(t1, t2);
    context.assertNotEquals(t1.hashCode(), t2.hashCode());

    context.assertNotEquals(t3, t4);
    context.assertNotEquals(t3.hashCode(), t4.hashCode());

    context.assertNotEquals(t3, t5);
    context.assertNotEquals(t3.hashCode(), t5.hashCode());

    context.assertFalse(t1.equals(null));
    context.assertFalse(t1.equals(t5));
  }
}
