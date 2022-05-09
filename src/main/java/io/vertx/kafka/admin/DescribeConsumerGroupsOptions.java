package io.vertx.kafka.admin;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

@DataObject(generateConverter = true)
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
