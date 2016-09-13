package io.vertx.kafka.consumer;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
@DataObject
public class ConsumerOptions {

  private boolean workerThread = true;

  public ConsumerOptions() {
  }

  public ConsumerOptions(JsonObject json) {
  }

  public boolean isWorkerThread() {
    return workerThread;
  }

  public ConsumerOptions setWorkerThread(boolean workerThread) {
    this.workerThread = workerThread;
    return this;
  }
}
