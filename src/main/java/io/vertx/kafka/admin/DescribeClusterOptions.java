package io.vertx.kafka.admin;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

@DataObject(generateConverter = true)
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
