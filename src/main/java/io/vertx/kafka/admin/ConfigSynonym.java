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
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource;

/**
 * Class representing a configuration synonym of a {@link ConfigEntry}
 */
@DataObject
@JsonGen(publicConverter = false)
public class ConfigSynonym {

  private String name;
  private String value;
  private ConfigSource source;

  /**
   * Constructor
   */
  public ConfigSynonym() {

  }

  /**
   * Constructor
   *
   * @param name the name of this configuration
   * @param value the value of this configuration, which may be null if the configuration is sensitive
   * @param source the source of this configuration
   */
  public ConfigSynonym(String name, String value, ConfigSource source) {
    this.name = name;
    this.value = value;
    this.source = source;
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public ConfigSynonym(JsonObject json) {

    ConfigSynonymConverter.fromJson(json, this);
  }

  /**
   * @return the name of this configuration
   */
  public String getName() {
    return name;
  }

  /**
   * Set the name of this configuration
   *
   * @param name the name of this configuration
   * @return current instance of the class to be fluent
   */
  public ConfigSynonym setName(String name) {
    this.name = name;
    return this;
  }

  /**
   * @return the value of this configuration, which may be null if the configuration is sensitive
   */
  public String getValue() {
    return value;
  }

  /**
   * Set the value of this configuration, which may be null if the configuration is sensitive
   *
   * @param value the value of this configuration, which may be null if the configuration is sensitive
   * @return current instance of the class to be fluent
   */
  public ConfigSynonym setValue(String value) {
    this.value = value;
    return this;
  }

  /**
   * @return the source of this configuration
   */
  public ConfigSource getSource() {
    return source;
  }

  /**
   * Set the source of this configuration
   *
   * @param source the source of this configuration
   * @return current instance of the class to be fluent
   */
  public ConfigSynonym setSource(ConfigSource source) {
    this.source = source;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {

    JsonObject json = new JsonObject();
    ConfigSynonymConverter.toJson(this, json);
    return json;
  }

  @Override
  public String toString() {

    return "ConfigSynonym{" +
      "name=" + this.name +
      ",value=" + this.value +
      ",source=" + this.source +
      "}";
  }
}
