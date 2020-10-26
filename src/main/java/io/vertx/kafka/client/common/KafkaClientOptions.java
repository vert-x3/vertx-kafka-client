/*
 * Copyright 2020 Red Hat Inc.
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
package io.vertx.kafka.client.common;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Generic KafkaClient options.
 */
@DataObject(generateConverter = true)
public class KafkaClientOptions {

  private Map<String, Object> config;

  public KafkaClientOptions() {
  }

  public KafkaClientOptions(JsonObject json) {
  }

  /**
   * @return the kafka config
   */
  public Map<String, Object> getConfig() {
    return config;
  }

  /**
   * Set the Kafka config.
   *
   * @param config the config
   * @return a reference to this, so the API can be used fluently
   */
  public KafkaClientOptions setConfig(Map<String, Object> config) {
    this.config = config;
    return this;
  }

  /**
   * Set a Kafka config entry.
   *
   * @param key the config key
   * @param value the config value
   * @return a reference to this, so the API can be used fluently
   */
  @GenIgnore
  public KafkaClientOptions setConfig(String key, Object value) {
    if (config == null) {
      config = new HashMap<>();
    }
    config.put(key, value);
    return this;
  }

  public JsonObject toJson() {
    return new JsonObject();
  }

}
