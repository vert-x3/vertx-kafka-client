package io.vertx.kafka.client.consumer.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.function.BiConsumer;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class EventLoopThreadConsumer<K, V> extends KafkaReadStreamBase<K, V> {

  private final Consumer<K, V> consumer;

  public EventLoopThreadConsumer(Context context, Consumer<K, V> consumer) {
    super(context);
    this.consumer = consumer;
  }

  @Override
  protected <T> void start(BiConsumer<Consumer, Future<T>> task, Handler<AsyncResult<T>> handler) {
    this.executeTask(task, handler);
  }

  @Override
  protected <T> void executeTask(BiConsumer<Consumer, Future<T>> task, Handler<AsyncResult<T>> handler) {

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
  }

  @Override
  protected void poll(Handler<ConsumerRecords<K, V>> handler) {
    handler.handle(this.consumer.poll(0));
  }

  @Override
  protected void doClose(Handler<Void> completionHandler) {
    this.context.runOnContext(v -> {

      this.consumer.close();
      if (completionHandler != null) {
        completionHandler.handle(null);
      }
    });
  }
}
