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
 * An update to the number of partitions including assignment.
 * Partitions can be increased only. If decrease, an exception from Kafka broker is received.
 * If no assignment is specifies brokers will randomly assign the partitions.
 */
@DataObject
@JsonGen(publicConverter = false)
public class NewPartitions {

    private int totalCount;

    private List<List<Integer>> newAssignments;

    /**
     * Constructor
     */
    public NewPartitions() {

    }


    /**
     * Constructor
     *
     * @param totalCount total count of partitions
     * @param newAssignments assignment to the brokers
     */
    public NewPartitions(int totalCount, List<List<Integer>> newAssignments) {
        this.totalCount = totalCount;
        this.newAssignments = newAssignments;
    }

    /**
     * Constructor
     *
     * @param totalCount total count of partitions
     */
    public void NewPartitions(int totalCount) {
        this.totalCount = totalCount;
        this.newAssignments = null;
    }


    /**
     * Constructor (from JSON representation)
     *
     * @param json  JSON representation
     */
    public NewPartitions(JsonObject json) {

        NewPartitionsConverter.fromJson(json, this);
    }


    /**
     * Set the number of partitions for the topic
     * @param totalCount the number of partitions for the topic
     * @return current instance of the class to be fluent
     */
    public NewPartitions setTotalCount(int totalCount) {
        this.totalCount = totalCount;
        return this;
    }

    /**
     * Set the assignment for the new partitions
     * @param assignments assignments of the partitions to the brokers
     * @return current instance of the class to be fluent
     */
    public NewPartitions setNewAssignments(List<List<Integer>> assignments) {
        this.newAssignments = assignments;
        return this;
    }

    /**
     * @return number of total partitions
     */
    public int getTotalCount() {
        return this.totalCount;
    }

    /**
     * @return assignment of partitions to the brokers
     */
    public List<List<Integer>> getNewAssignments() {
        return this.newAssignments;
    }

    /**
     * Convert object to JSON representation
     *
     * @return  JSON representation
     */
    public JsonObject toJson() {

        JsonObject json = new JsonObject();
        NewPartitionsConverter.toJson(this, json);
        return json;
    }

    @Override
    public String toString() {
        return "NewPartitions{" +
                "totalCount=" + this.totalCount +
                ",newAssignments=" + this.newAssignments +
                "}";
    }

}
