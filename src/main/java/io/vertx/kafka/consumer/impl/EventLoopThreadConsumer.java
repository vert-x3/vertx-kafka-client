package io.vertx.kafka.consumer.impl;

import io.vertx.core.Context;
import io.vertx.core.Handler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

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
  protected void start(java.util.function.Consumer<Consumer> task) {
    task.accept(consumer);
  }

  @Override
  protected void executeTask(java.util.function.Consumer<Consumer> task) {
    task.accept(consumer);
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
