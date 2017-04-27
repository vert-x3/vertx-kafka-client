package io.vertx.kafka.admin.impl;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

@VertxGen
public class AdminUtilsImpl implements AdminUtils {
  private Vertx vertx;
  private final String zookeeper;
  private final boolean isSecure;
  private int connectionTimeoutMs;
  private ZkUtils zkUtils;
  private boolean autoClose = false;

  public AdminUtilsImpl(Vertx vertx, String zookeeperHosts, int connectionTimeoutMs, boolean isSecure, boolean autoClose) {
    this.vertx = vertx;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.zookeeper = zookeeperHosts;
    this.isSecure = isSecure;
    this.autoClose = autoClose;
  }

  public AdminUtilsImpl(Vertx vertx, String zookeeperHosts) {
   this(vertx, zookeeperHosts, 8000, false, false);
  }

  public AdminUtilsImpl(Vertx vertx, String zookeeperHosts, boolean autoClose) {
    this(vertx, zookeeperHosts, 8000, false, autoClose);
  }

  @Override
  public void createTopic(String topicName, int partitionCount, int replicationFactor,
                          Handler<AsyncResult<Void>> completionHandler) {
    createTopic(topicName, partitionCount, replicationFactor, new HashMap<>(), completionHandler);
  }

  @Override
  public void createTopic(String topicName, int partitionCount, int replicationFactor,
                          Map<String, String> topicConfig,
                          Handler<AsyncResult<Void>> completionHandler) {
    Properties topicConfigProperties = new Properties();
    topicConfigProperties.putAll(topicConfig);

    vertx.executeBlocking(future -> {
      try {
        kafka.admin.AdminUtils.createTopic(initZkClientAndGetZkUtils(), topicName, partitionCount, replicationFactor, topicConfigProperties,
          kafka.admin.AdminUtils.createTopic$default$6());
        // Note $default$6() retrieves the default parameter 6 from the Scala method createTopic -- RackAwareMode
        // As of Kafka 0.10.1.1, this is equivalent to RackAwareMode.Disabled$.MODULE$
        completionHandler.handle(Future.succeededFuture());
      } catch(Exception e) {
        completionHandler.handle(Future.failedFuture(e.getLocalizedMessage()));
      }
      finally {
        if(autoClose) {
          zkUtils.close();
        }
      }
    }, r -> {
    });
  }

  @Override
  public void deleteTopic(String topicName,
                          Handler<AsyncResult<Void>> completionHandler) {
    vertx.executeBlocking(future -> {
      try {
        kafka.admin.AdminUtils.deleteTopic(initZkClientAndGetZkUtils(), topicName);
        completionHandler.handle(Future.succeededFuture());
        future.complete();
      } catch(Exception e) {
        completionHandler.handle(Future.failedFuture(e.getLocalizedMessage()));
        future.fail(e);
      }
      finally {
        if(autoClose) {
          zkUtils.close();
        }
      }
    }, r -> {
    });
  }

  @Override
  public void topicExists(String topicName,
                          Handler<AsyncResult<Boolean>> completionHandler) {
    vertx.executeBlocking(future -> {
      try {
        boolean exists = kafka.admin.AdminUtils.topicExists(initZkClientAndGetZkUtils(), topicName);
        completionHandler.handle(Future.succeededFuture(exists));
        future.complete();
      } catch(Exception e) {
        completionHandler.handle(Future.failedFuture(e.getLocalizedMessage()));
        future.fail(e);
      }
      finally {
        if(autoClose) {
          zkUtils.close();
        }
      }
    }, r -> {
    });
  }

  @Override
  public void changeTopicConfig(String topicName, Map<String, String> topicConfig,
                          Handler<AsyncResult<Void>> completionHandler) {
    Properties topicConfigProperties = new Properties();
    topicConfigProperties.putAll(topicConfig);

    vertx.executeBlocking(future -> {
      try {
        kafka.admin.AdminUtils.changeTopicConfig(initZkClientAndGetZkUtils(), topicName, topicConfigProperties);
        completionHandler.handle(Future.succeededFuture());
      } catch(Exception e) {
        completionHandler.handle(Future.failedFuture(e.getLocalizedMessage()));
      }
      finally {
        if(autoClose) {
          zkUtils.close();
        }
      }
    }, r -> {
    });
  }

  public void close(Handler<AsyncResult<Void>> completionHandler) {
    vertx.executeBlocking(future -> {
      if(zkUtils != null)
        zkUtils.close();

      completionHandler.handle(Future.succeededFuture());
      future.complete();
    }, r -> {});

  }

  /*
    Utility method to create a ZKUtils instance with an attached Zookeeper client
   */
  private synchronized ZkUtils initZkClientAndGetZkUtils() {
    int sessionTimeoutMs = 10 * 1000;
    // see http://stackoverflow.com/questions/16946778/how-can-we-create-a-topic-in-kafka-from-the-ide-using-api
    // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
    // createTopic() will only seem to work (it will return without error).  The topic will exist in
    // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
    // topic.
    if (zkUtils == null) {
      zkUtils = new ZkUtils(
        new ZkClient(
          zookeeper,
          sessionTimeoutMs,
          connectionTimeoutMs,
          ZKStringSerializer$.MODULE$), new ZkConnection(zookeeper), isSecure);
    }
    return zkUtils;
  }
}
