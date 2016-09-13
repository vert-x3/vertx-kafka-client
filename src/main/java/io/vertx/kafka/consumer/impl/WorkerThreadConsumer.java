package io.vertx.kafka.consumer.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
  protected void start(java.util.function.Consumer<Consumer> task, Handler<AsyncResult<Void>> completionHandler) {
    worker = Executors.newSingleThreadExecutor();
    executeTask(task, completionHandler);
  }

  @Override
  protected void executeTask(java.util.function.Consumer<Consumer> task, Handler<AsyncResult<Void>> completionHandler) {
    worker.submit(() -> {
      try {
        task.accept(consumer);
      } catch (Exception e) {
        if (completionHandler != null) {
          context.runOnContext(v -> completionHandler.handle(Future.failedFuture(e)));
        }
        return;
      }
      if (completionHandler != null) {
        context.runOnContext(v -> completionHandler.handle(Future.succeededFuture()));
      }
    });
  }

  @Override
  protected void poll(Handler<ConsumerRecords<K, V>> handler) {
    worker.submit(run(handler));
  }

  private Runnable run(Handler<ConsumerRecords<K, V>> handler) {
    return () -> {
      if (!closed.get()) {
        try {
          ConsumerRecords<K, V> records = consumer.poll(1000);
          if (records != null && records.count() > 0) {
            context.runOnContext(v -> handler.handle(records));
          } else {
            poll(handler);
          }
        } catch (WakeupException ignore) {
        }
      }
    };
  }

  @Override
  protected void doClose(Handler<Void> completionHandler) {
    worker.submit(() -> {
      consumer.close();
      context.runOnContext(v -> completionHandler.handle(null));
    });
    consumer.wakeup();
  }
}
