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
public class DescribeClusterOptions {

    private boolean includeAuthorizedOperations;

    /**
     * Constructor
     */
    public DescribeClusterOptions() {}


    /**
     * Constructor (from JSON representation)
     *
     * @param json  JSON representation
     */
    public DescribeClusterOptions(JsonObject json) {
        DescribeClusterOptionsConverter.fromJson(json, this);
    }

    public DescribeClusterOptions includeAuthorizedOperations(boolean includeAuthorizedOperations) {
        this.includeAuthorizedOperations = includeAuthorizedOperations;
        return this;
    }

    /**
     * Specify if authorized operations should be included in the response.  Note that some
     * older brokers cannot not supply this information even if it is requested.
     */
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
        DescribeClusterOptionsConverter.toJson(this, json);
        return json;
    }

    @Override
    public String toString() {
        return "DescribeClusterOptions{" +
        "includeAuthorizedOperations=" + includeAuthorizedOperations +
        '}';
    }

}
