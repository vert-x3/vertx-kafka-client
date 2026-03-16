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

import io.vertx.core.buffer.Buffer;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

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

  @Test
  public void testHeaderWithNullByteArrayValue() {
    // Test creating header with null byte[] value (per Kafka spec)
    byte[] nullBytes = null;
    KafkaHeader header = KafkaHeader.header("test-key", nullBytes);

    assertNotNull(header);
    assertEquals("test-key", header.key());
    assertNull(header.value());
  }

  @Test
  public void testHeaderWithNullBufferValue() {
    // Test creating header with null Buffer value
    Buffer nullBuffer = null;
    KafkaHeader header = KafkaHeader.header("test-key", nullBuffer);

    assertNotNull(header);
    assertEquals("test-key", header.key());
    assertNull(header.value());
  }

  @Test
  public void testHeaderWithNullStringValue() {
    // Test creating header with null String value
    String nullString = null;
    KafkaHeader header = KafkaHeader.header("test-key", nullString);

    assertNotNull(header);
    assertEquals("test-key", header.key());
    assertNull(header.value());
  }

  @Test
  public void testHeaderEqualsWithNullValues() {
    // Test equals() method with null values
    KafkaHeader header1 = KafkaHeader.header("key", (Buffer) null);
    KafkaHeader header2 = KafkaHeader.header("key", (Buffer) null);
    KafkaHeader header3 = KafkaHeader.header("key", "value");

    assertEquals(header1, header2);
    assertEquals(header1.hashCode(), header2.hashCode());
    assertNotEquals(header1, header3);
    assertNotEquals(header1.hashCode(), header3.hashCode());
  }

  @Test
  public void testHeaderToStringWithNullValue() {
    // Test toString() method with null value
    KafkaHeader header = KafkaHeader.header("test-key", (Buffer) null);
    String result = header.toString();

    assertNotNull(result);
    // Should not throw NPE
  }

  @Test
  public void testRecordWithNullHeaderValues() {
    // Test adding headers with null values to a producer record
    List<KafkaHeader> headers = Arrays.asList(
      KafkaHeader.header("key1", "value1"),
      KafkaHeader.header("key2", (String) null),
      KafkaHeader.header("key3", (byte[]) null)
    );

    List<KafkaHeader> recordHeaders =
      KafkaProducerRecord.create("mytopic", "mykey", "myvalue").addHeaders(headers).headers();

    assertNotNull(recordHeaders);
    assertEquals(3, recordHeaders.size());

    KafkaHeader kafkaHeader1 = recordHeaders.get(0);
    assertEquals("key1", kafkaHeader1.key());
    assertEquals("value1", kafkaHeader1.value().toString());

    KafkaHeader kafkaHeader2 = recordHeaders.get(1);
    assertEquals("key2", kafkaHeader2.key());
    assertNull(kafkaHeader2.value());

    KafkaHeader kafkaHeader3 = recordHeaders.get(2);
    assertEquals("key3", kafkaHeader3.key());
    assertNull(kafkaHeader3.value());
  }

}
