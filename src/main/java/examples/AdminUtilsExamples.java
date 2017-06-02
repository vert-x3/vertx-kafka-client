package examples;

import java.util.HashMap;
import java.util.Map;

import io.vertx.core.Vertx;
import io.vertx.docgen.Source;
import io.vertx.kafka.admin.AdminUtils;

@Source
public class AdminUtilsExamples {

  public void createTopic() {
    AdminUtils adminUtils = AdminUtils.create(Vertx.vertx(), "localhost:2181", true);
    // Create topic 'myNewTopic' with 2 partition and 1 replicas
    adminUtils.createTopic("myNewTopic", 2, 1, result -> {
      if(result.succeeded())
        System.out.println("Creation of topic myNewTopic successful!");
      else
        System.out.println("Creation of topic myNewTopic failed: "+
          result.cause().getLocalizedMessage());
    });
  }

  public void deleteTopic() {
    AdminUtils adminUtils = AdminUtils.create(Vertx.vertx(), "localhost:2181", true);
    // Delete topic 'myNewTopic'
    adminUtils.deleteTopic("myNewTopic", result -> {
      if(result.succeeded())
        System.out.println("Deletion of topic myNewTopic successful!");
      else
        System.out.println("Deletion of topic myNewTopic failed: "+
          result.cause().getLocalizedMessage());
    });
  }

  public void changeTopicConfig() {
    AdminUtils adminUtils = AdminUtils.create(Vertx.vertx(), "localhost:2181", true);
    // Set retention to 1000 ms and max size of the topic partition to 1 kiByte
    Map<String, String> properties = new HashMap<>();
    properties.put("delete.retention.ms", "1000");
    properties.put("retention.bytes", "1024");
    adminUtils.changeTopicConfig("myNewTopic", properties, result -> {
      if(result.succeeded())
        System.out.println("Configuration change of topic myNewTopic successful!");
      else
        System.out.println("Configuration change of topic myNewTopic failed: "+
          result.cause().getLocalizedMessage());
    });
  }

  public void topicExists() {
    AdminUtils adminUtils = AdminUtils.create(Vertx.vertx(), "localhost:2181", true);
    adminUtils.topicExists("myNewTopic", result -> {
      if(result.succeeded()) {
        System.out.println("Topic myNewTopic exists: "+result.result());
      }
      else
        System.out.println("Failed to check if topic myNewTopic exists: "+
          result.cause().getLocalizedMessage());
    });
  }
}
