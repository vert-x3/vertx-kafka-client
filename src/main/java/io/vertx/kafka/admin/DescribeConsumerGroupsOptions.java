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
import io.vertx.codegen.annotations.JsonGen;
import io.vertx.core.json.JsonObject;

@DataObject
@JsonGen(publicConverter = false)
public class DescribeConsumerGroupsOptions {
    private boolean includeAuthorizedOperations;

    /**
     * Constructor
     */
    public DescribeConsumerGroupsOptions() {}


    /**
     * Constructor (from JSON representation)
     *
     * @param json  JSON representation
     */
    public DescribeConsumerGroupsOptions(JsonObject json) {
        DescribeConsumerGroupsOptionsConverter.fromJson(json, this);
    }

    public DescribeConsumerGroupsOptions includeAuthorizedOperations(boolean includeAuthorizedOperations) {
        this.includeAuthorizedOperations = includeAuthorizedOperations;
        return this;
    }

    public boolean includeAuthorizedOperations() {
        return includeAuthorizedOperations;
    }

    /**
     * Convert object to JSON representation
     *
     * @return  JSON representation
     */
    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        DescribeConsumerGroupsOptionsConverter.toJson(this, json);
        return json;
    }

    @Override
    public String toString() {
        return "DescribeConsumerGroupsOptions{" +
        "includeAuthorizedOperations=" + includeAuthorizedOperations +
        '}';
    }
}
