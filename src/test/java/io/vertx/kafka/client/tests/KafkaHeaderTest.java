/*
 * Copyright 2018 Red Hat Inc.
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

import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class KafkaHeaderTest {

  @Test
  public void testEmptyHeaders() {
    List<KafkaHeader> kafkaHeaders = KafkaProducerRecord.create("topic", "key", "value").headers();
    assertEquals(Collections.emptyList(), kafkaHeaders);
  }

  @Test
  public void testRecordWithHeaders() {
    List<KafkaHeader> headers = Arrays.asList(
      KafkaHeader.header("key1", "value1"),
      KafkaHeader.header("key2", "value2")
    );

    List<KafkaHeader> recordHeaders =
      KafkaProducerRecord.create("mytopic", "mykey", "myvalue").addHeaders(headers).headers();

    assertNotNull(recordHeaders);
    assertEquals(2, recordHeaders.size());

    KafkaHeader kafkaHeader1 = recordHeaders.get(0);
    assertEquals("key1", kafkaHeader1.key());
    assertEquals("value1", kafkaHeader1.value().toString());

    KafkaHeader kafkaHeader2 = recordHeaders.get(1);
    assertEquals("key2", kafkaHeader2.key());
    assertEquals("value2", kafkaHeader2.value().toString());
  }

}
