package io.vertx.kafka.consumer.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.kafka.consumer.KafkaConsumer;
import io.vertx.kafka.consumer.KafkaConsumerRecord;
import io.vertx.kafka.consumer.KafkaReadStream;

import java.util.Set;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class KafkaConsumerImpl implements KafkaConsumer {

  private final KafkaReadStream<?, ?> stream;

  public KafkaConsumerImpl(KafkaReadStream<?, ?> stream) {
    this.stream = stream;
  }

  @Override
  public KafkaConsumer exceptionHandler(Handler<Throwable> handler) {
    stream.exceptionHandler(handler);
    return this;
  }

  @Override
  public KafkaConsumer handler(Handler<KafkaConsumerRecord> handler) {
    if (handler != null) {
      stream.handler(record -> handler.handle(new KafkaConsumerRecordImpl(record)));
    } else {
      stream.handler(null);
    }
    return this;
  }

  @Override
  public KafkaConsumer pause() {
    stream.pause();
    return this;
  }

  @Override
  public KafkaConsumer resume() {
    stream.resume();
    return this;
  }

  @Override
  public KafkaConsumer endHandler(Handler<Void> endHandler) {
    stream.endHandler(endHandler);
    return this;
  }

  @Override
  public KafkaConsumer subscribe(Set<String> topics) {
    stream.subscribe(topics);
    return this;
  }

  @Override
  public KafkaConsumer subscribe(Set<String> topics, Handler<AsyncResult<Void>> handler) {
    stream.subscribe(topics, handler);
    return this;
  }

  @Override
  public void commit() {
    stream.commit();
  }

  @Override
  public void commit(Handler<AsyncResult<Void>> completionHandler) {
    stream.commit(completionHandler != null ? ar -> {
      if (ar.succeeded()) {
        completionHandler.handle(Future.succeededFuture());
      } else {
        completionHandler.handle(Future.failedFuture(ar.cause()));
      }
    } : null);
  }

  @Override
  public void close(Handler<Void> completionHandler) {
    stream.close(completionHandler);
  }
}
