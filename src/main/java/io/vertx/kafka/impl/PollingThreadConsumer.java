package io.vertx.kafka.impl;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Map;
import java.util.Properties;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class PollingThreadConsumer<K, V> extends KafkaConsumerBase<K, V> {

  public static <K, V> KafkaConsumerBase<K, V> create(Vertx vertx, Properties config) {
    return new PollingThreadConsumer<>(vertx.getOrCreateContext(), new org.apache.kafka.clients.consumer.KafkaConsumer<>(config));
  }


  public static <K, V> KafkaConsumerBase<K, V> create(Vertx vertx, Map<String, Object> config) {
    return new PollingThreadConsumer<>(vertx.getOrCreateContext(), new org.apache.kafka.clients.consumer.KafkaConsumer<>(config));
  }

  public static <K, V> KafkaConsumerBase<K, V> create(Vertx vertx, Consumer<K, V> consumer) {
    return new PollingThreadConsumer<>(vertx.getOrCreateContext(), consumer);
  }

  private Thread pollingThread = new Thread(this::run);
  private volatile ConsumerRecords<K, V> records;
  private Future<Void> closeFuture = Future.future();

  private PollingThreadConsumer(Context context, Consumer<K, V> consumer) {
    super(context, consumer);
  }

  @Override
  protected void start() {
    pollingThread.start();
    super.start();
  }

  @Override
  protected ConsumerRecords<K, V> fetchRecords() {
    ConsumerRecords<K, V> ret = records;
    records = null;
    return ret;
  }

  private void run() {
    try {
      while (!closed.get()) {
        if (records == null) {
          try {
            ConsumerRecords<K, V> polled = consumer.poll(1000);
            if (polled.count() > 0) {
              records = polled;
            }
          } catch (WakeupException e) {
            continue;
          }
        } else {
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
    } finally {
      consumer.close();
      context.runOnContext(closeFuture::complete);
    }
  }

  @Override
  protected void doClose(Handler<Void> completionHandler) {
    if (completionHandler != null) {
      closeFuture.setHandler(ar -> completionHandler.handle(null));
    }
    consumer.wakeup();
  }
}
