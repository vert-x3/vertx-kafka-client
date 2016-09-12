package io.vertx.kafka;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class ConsumerOptions {

  private boolean workerThread = false;

  public ConsumerOptions() {
  }

  public boolean isWorkerThread() {
    return workerThread;
  }

  public ConsumerOptions setWorkerThread(boolean workerThread) {
    this.workerThread = workerThread;
    return this;
  }
}
