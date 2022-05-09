package io.vertx.kafka.admin;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

@DataObject(generateConverter = true)
public class DescribeTopicsOptions {
    private boolean includeAuthorizedOperations;

    /**
     * Constructor
     */
    public DescribeTopicsOptions() {}

    /**
     * Constructor (from JSON representation)
     *
     * @param json  JSON representation
     */
    public DescribeTopicsOptions(JsonObject json) {
        DescribeTopicsOptionsConverter.fromJson(json, this);
    }

    public DescribeTopicsOptions includeAuthorizedOperations(boolean includeAuthorizedOperations) {
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
        DescribeTopicsOptionsConverter.toJson(this, json);
        return json;
    }

    @Override
    public String toString() {
        return "DescribeConsumerGroupsOptions{" +
        "includeAuthorizedOperations=" + includeAuthorizedOperations +
        '}';
    }
}
