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

import java.util.List;

/**
 * A class representing a configuration entry containing name, value and additional metadata
 */
@DataObject
@JsonGen(publicConverter = false)
public class ConfigEntry {

  private String name;
  private boolean isDefault;
  private boolean isReadOnly;
  private boolean isSensitive;
  private ConfigSource source;
  private List<ConfigSynonym> synonyms;
  private String value;

  /**
   * Constructor
   */
  public ConfigEntry() {

  }

  /**
   * Constructor
   *
   * @param name the non-null config name
   * @param value the config value or null
   */
  public ConfigEntry(String name, String value) {
    this.name = name;
    this.value = value;
  }

  /**
   * Constructor
   * 
   * @param name the non-null config name  
   * @param value the config value or null
   * @param source the source of this config entry 
   * @param isSensitive whether the config value is sensitive, the broker never returns the value if it is sensitive
   * @param isReadOnly whether the config is read-only and cannot be updated
   * @param synonyms Synonym configs in order of precedence
   */
  public ConfigEntry(String name, String value, ConfigSource source, boolean isSensitive, 
                    boolean isReadOnly, List<ConfigSynonym> synonyms) {
    this.name = name;
    this.value = value;
    this.source = source;
    this.isSensitive = isSensitive;
    this.isReadOnly = isReadOnly;
    this.synonyms = synonyms;
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public ConfigEntry(JsonObject json) {

    ConfigEntryConverter.fromJson(json, this);
  }

  /**
   * @return the config name
   */
  public String getName() {
    return name;
  }

  /**
   * Set the config name
   *
   * @param name the config name.
   * @return current instance of the class to be fluent
   */
  public ConfigEntry setName(String name) {
    this.name = name;
    return this;
  }

  /**
   * @return whether the config value is the default or if it's been explicitly set
   */
  public boolean isDefault() {
    return isDefault;
  }

  /**
   * Set whether the config value is the default or if it's been explicitly set
   *
   * @param isDefault whether the config value is the default or if it's been explicitly set
   * @return current instance of the class to be fluent
   */
  public ConfigEntry setDefault(boolean isDefault) {
    this.isDefault = isDefault;
    return this;
  }

  /**
   * @return whether the config is read-only and cannot be updated
   */
  public boolean isReadOnly() {
    return isReadOnly;
  }

  /**
   * Set whether the config is read-only and cannot be updated
   *
   * @param readOnly whether the config is read-only and cannot be updated
   * @return current instance of the class to be fluent
   */
  public ConfigEntry setReadOnly(boolean readOnly) {
    isReadOnly = readOnly;
    return this;
  }

  /**
   * @return whether the config value is sensitive. The value is always set to null by the broker if the config value is sensitive
   */
  public boolean isSensitive() {
    return isSensitive;
  }

  /**
   * Set whether the config value is sensitive. The value is always set to null by the broker if the config value is sensitive
   *
   * @param isSensitive whether the config value is sensitive. The value is always set to null by the broker if the config value is sensitive
   * @return current instance of the class to be fluent
   */
  public ConfigEntry setSensitive(boolean isSensitive) {
    this.isSensitive = isSensitive;
    return this;
  }

  /**
   * @return the source of this configuration entry
   */
  public ConfigSource getSource() {
    return source;
  }

  /**
   * Set the source of this configuration entry
   *
   * @param source the source of this configuration entry
   * @return current instance of the class to be fluent
   */
  public ConfigEntry setSource(ConfigSource source) {
    this.source = source;
    return this;
  }

  /**
   * @return all config values that may be used as the value of this config along with their source, in the order of precedence
   */
  public List<ConfigSynonym> getSynonyms() {
    return synonyms;
  }

  /**
   * Set all config values that may be used as the value of this config along with their source, in the order of precedence
   *
   * @param synonyms all config values that may be used as the value of this config along with their source, in the order of precedence
   * @return current instance of the class to be fluent
   */
  public ConfigEntry setSynonyms(List<ConfigSynonym> synonyms) {
    this.synonyms = synonyms;
    return this;
  }

  /**
   * @return the value or null. Null is returned if the config is unset or if isSensitive is true
   */
  public String getValue() {
    return value;
  }

  /**
   * Set the value or null. Null is returned if the config is unset or if isSensitive is true
   *
   * @param value the value or null. Null is returned if the config is unset or if isSensitive is true
   * @return current instance of the class to be fluent
   */
  public ConfigEntry setValue(String value) {
    this.value = value;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {

    JsonObject json = new JsonObject();
    ConfigEntryConverter.toJson(this, json);
    return json;
  }

  @Override
  public String toString() {

    return "ConfigEntry{" +
      "name=" + this.name +
      ",value=" + this.value +
      ",source=" + this.source +
      ",isDefault=" + this.isDefault +
      ",isSensitive=" + this.isSensitive +
      ",isReadOnly=" + this.isReadOnly +
      ",synonyms=" + this.synonyms +
      "}";
  }
}
