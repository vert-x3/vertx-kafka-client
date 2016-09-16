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
    executeTask(task, handler);
  }

  @Override
  protected <T> void executeTask(BiConsumer<Consumer, Future<T>> task, Handler<AsyncResult<T>> handler) {
    Future<T> fut;
    if (handler != null) {
      fut = Future.future();
      fut.setHandler(handler);
    } else {
      fut = null;
    }
    try {
      task.accept(consumer, fut);
    } catch (Exception e) {
      if (fut != null && !fut.isComplete()) {
        fut.fail(e);
      }
    }
  }

  @Override
  protected void poll(Handler<ConsumerRecords<K, V>> handler) {
    handler.handle(consumer.poll(0));
  }

  @Override
  protected void doClose(Handler<Void> completionHandler) {
    context.runOnContext(v -> {
      consumer.close();
      if (completionHandler != null) {
        completionHandler.handle(null);
      }
    });
  }
}
