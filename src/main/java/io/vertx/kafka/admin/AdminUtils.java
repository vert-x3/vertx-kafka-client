package io.vertx.kafka.admin;

import java.util.Map;
import java.util.Properties;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.admin.impl.AdminUtilsImpl;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;

/**
 * Provides a wrapper around important methods in Kafka's AdminUtils, namely
 * @see kafka.admin.AdminUtils#createTopic(ZkUtils, String, int, int, Properties, RackAwareMode),
 * @see kafka.admin.AdminUtils#deleteTopic(ZkUtils, String),
 * @see kafka.admin.AdminUtils#changeTopicConfig(ZkUtils, String, Properties), and
 * @see kafka.admin.AdminUtils#topicExists(ZkUtils, String)
 */
@VertxGen
public interface AdminUtils {

  /**
   * Create a new AdminUtils instance
   *
   * @param vertx Vert.x instance to use
   * @param zookeeperHosts comma-separated list of Zookeeper server, e.g. localhost:2181,localhost:2182
   * @return an instance of the AdminUtilWrapper
   */
  static AdminUtils create(Vertx vertx, String zookeeperHosts) {
    return new AdminUtilsImpl(vertx, zookeeperHosts);
  }

  /**
   * Create a new AdminUtils instance
   *
   * @param vertx Vert.x instance to use
   * @param zookeeperHosts comma-separated list of Zookeeper server, e.g. localhost:2181,localhost:2182
   * @param autoClose If set to true, the client will auto-close the connection after a command
   * @return an instance of the AdminUtilWrapper
   */
  static AdminUtils create(Vertx vertx, String zookeeperHosts, boolean autoClose) {
    return new AdminUtilsImpl(vertx, zookeeperHosts, autoClose);
  }
  /**
   * Create a new AdminUtils instance
   *
   * @param vertx Vert.x instance to use
   * @param zookeeperHosts comma-separated list of Zookeeper server, e.g. localhost:2181,localhost:2182
   * @param connectionTimeoutMs Maximum time in ms to wait for the client to connect to Zookeeper
   * @param isSecure If set to true, ZkUtils will perform security checks, i.e. ACL checks
   * @param autoClose If set to true, the client will auto-close the connection after a command
   * @return an instance of the AdminUtilWrapper
   */
  static AdminUtils create(Vertx vertx, String zookeeperHosts, int connectionTimeoutMs, boolean isSecure, boolean autoClose) {
    return new AdminUtilsImpl(vertx, zookeeperHosts, connectionTimeoutMs, isSecure, autoClose);
  }

  /**
   * Creates a new Kafka topic on all Brokers managed by the given Zookeeper instance(s)
   * @param topicName Name of the to-be-created topic
   * @param partitionCount Number of partitions
   * @param replicationFactor Number of replicates. Must be lower or equal to the number of available Brokers
   * @param completionHandler vert.x callback
   */
  void createTopic(String topicName, int partitionCount, int replicationFactor,
                   Handler<AsyncResult<Void>> completionHandler);

  /**
   * Creates a new Kafka topic on all Brokers managed by the given Zookeeper instance(s). In contrast
   * to @see {@link #createTopic(String, int, int, Handler)}, one can pass in additional configuration
   * parameters as a map (String -> String).
   * @param topicName Name of the to-be-created topic
   * @param partitionCount Number of partitions
   * @param replicationFactor Number of replicates. Must be lower or equal to the number of available Brokers
   * @param topicConfig map with additional topic configuration parameters
   * @param completionHandler vert.x callback
   */
  void createTopic(String topicName, int partitionCount, int replicationFactor,
                   Map<String, String> topicConfig,
                   Handler<AsyncResult<Void>> completionHandler);

  /**
   * Delete the Kafka topic given by the topicName.
   * @param topicName Name of the topic to be deleted
   * @param completionHandler vert.x callback
   */
  void deleteTopic(String topicName,
                   Handler<AsyncResult<Void>> completionHandler);

  /**
   * Checks if the Kafka topic given by topicName does exist.
   * @param topicName Name of the topic
   * @param completionHandler vert.x callback
   */
  void topicExists(String topicName,
                   Handler<AsyncResult<Boolean>> completionHandler);

  /**
   * Updates the configuration of the topic given by topicName. Configuration parameters
   * are passed in as a Map (Key -> Value) of Strings.
   * @param topicName topic to be configured
   * @param topicConfig Map with configuration items
   * @param completionHandler vert.x callback
   */
  void changeTopicConfig(String topicName, Map<String, String> topicConfig,
                         Handler<AsyncResult<Void>> completionHandler);

  /**
   * Closes the underlying connection to Zookeeper. It is required to call the method for cleanup
   * purposes if AdminUtils was not created with autoClose set to true.
   * @param completionHandler vert.x callback
   */
  void close(Handler<AsyncResult<Void>> completionHandler);
}
