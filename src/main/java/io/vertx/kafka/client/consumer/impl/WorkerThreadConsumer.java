package io.vertx.kafka.client.consumer.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class WorkerThreadConsumer<K, V> extends KafkaReadStreamBase<K, V> {


  private final Consumer<K, V> consumer;
  private ExecutorService worker;

  public WorkerThreadConsumer(Context context, Consumer<K, V> consumer) {
    super(context);
    this.consumer = consumer;
  }

  @Override
  protected <T> void start(BiConsumer<Consumer, Future<T>> task, Handler<AsyncResult<T>> handler) {

    this.worker = Executors.newSingleThreadExecutor();
    this.executeTask(task, handler);
  }

  @Override
  protected <T> void executeTask(BiConsumer<Consumer, Future<T>> task, Handler<AsyncResult<T>> handler) {

    this.worker.submit(() -> {

      Future<T> future;
      if (handler != null) {
        future = Future.future();
        future.setHandler(handler);
      } else {
        future = null;
      }
      try {
        task.accept(this.consumer, future);
      } catch (Exception e) {
        if (future != null && !future.isComplete()) {
          future.fail(e);
        }
      }
    });
  }

  @Override
  protected void poll(Handler<ConsumerRecords<K, V>> handler) {
    this.worker.submit(run(handler));
  }

  private Runnable run(Handler<ConsumerRecords<K, V>> handler) {

    return () -> {

      if (!this.closed.get()) {
        try {
          ConsumerRecords<K, V> records = this.consumer.poll(1000);
          if (records != null && records.count() > 0) {
            this.context.runOnContext(v -> handler.handle(records));
          } else {
            this.poll(handler);
          }
        } catch (WakeupException ignore) {
        }
      }
    };
  }

  @Override
  protected void doClose(Handler<Void> completionHandler) {

    this.worker.submit(() -> {

      this.consumer.close();
      if (completionHandler != null) {
        this.context.runOnContext(completionHandler);
      }
    });
    this.consumer.wakeup();
  }
}
