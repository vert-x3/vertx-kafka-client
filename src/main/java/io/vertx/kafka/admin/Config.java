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

package io.vertx.kafka.admin;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonObject;

import java.util.List;

/**
 * A configuration object containing the configuration entries for a resource
 */
@DataObject
@JsonGen(publicConverter = false)
public class Config {

  private List<ConfigEntry> entries;

  /**
   * Constructor
   */
  public Config() {

  }

  /**
   * Constructor
   *
   * @param entries configuration entries for a resource
   */
  public Config(List<ConfigEntry> entries) {
    this.entries = entries;
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public Config(JsonObject json) {

    ConfigConverter.fromJson(json, this);
  }

  /**
   * @return configuration entries for a resource
   */
  public List<ConfigEntry> getEntries() {
    return entries;
  }

  /**
   * Set the configuration entries for a resource
   *
   * @param entries configuration entries for a resource
   * @return current instance of the class to be fluent
   */
  public Config setEntries(List<ConfigEntry> entries) {
    this.entries = entries;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {

    JsonObject json = new JsonObject();
    ConfigConverter.toJson(this, json);
    return json;
  }

  @Override
  public String toString() {

    return "Config{" +
      "entries=" + this.entries +
      "}";
  }
}
