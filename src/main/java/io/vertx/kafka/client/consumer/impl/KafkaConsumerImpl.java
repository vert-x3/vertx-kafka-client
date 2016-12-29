package io.vertx.kafka.client.consumer.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.kafka.client.common.Helper;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaReadStream;

import java.util.Set;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class KafkaConsumerImpl<K, V> implements KafkaConsumer<K, V> {

  private final KafkaReadStream<K, V> stream;

  public KafkaConsumerImpl(KafkaReadStream<K, V> stream) {
    this.stream = stream;
  }

  @Override
  public KafkaConsumer<K, V> exceptionHandler(Handler<Throwable> handler) {
    this.stream.exceptionHandler(handler);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> handler(Handler<KafkaConsumerRecord<K, V>> handler) {
    if (handler != null) {
      this.stream.handler(record -> handler.handle(new KafkaConsumerRecordImpl<>(record)));
    } else {
      this.stream.handler(null);
    }
    return this;
  }

  @Override
  public KafkaConsumer<K, V> pause() {
    this.stream.pause();
    return this;
  }

  @Override
  public KafkaConsumer<K, V> resume() {
    this.stream.resume();
    return this;
  }

  @Override
  public KafkaConsumer<K, V> pause(Set<TopicPartition> topicPartitions) {
    return this.pause(topicPartitions, null);
  }

  @Override
  public KafkaConsumer<K, V> pause(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
    this.stream.pause(Helper.to(topicPartitions), completionHandler);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> resume(Set<TopicPartition> topicPartitions) {
    return this.resume(topicPartitions, null);
  }

  @Override
  public KafkaConsumer<K, V> resume(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
    this.stream.resume(Helper.to(topicPartitions), completionHandler);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> endHandler(Handler<Void> endHandler) {
    this.stream.endHandler(endHandler);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> subscribe(Set<String> topics) {
    this.stream.subscribe(topics);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> subscribe(Set<String> topics, Handler<AsyncResult<Void>> completionHandler) {
    this.stream.subscribe(topics, completionHandler);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> unsubscribe() {
    this.stream.unsubscribe();
    return this;
  }

  @Override
  public KafkaConsumer<K, V> unsubscribe(Handler<AsyncResult<Void>> completionHandler) {
    this.stream.unsubscribe(completionHandler);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> subscription(Handler<AsyncResult<Set<String>>> handler) {
    this.stream.subscription(handler);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> partitionsRevokedHandler(Handler<Set<TopicPartition>> handler) {
    this.stream.partitionsRevokedHandler(Helper.adaptHandler(handler));
    return this;
  }

  @Override
  public KafkaConsumer<K, V> partitionsAssignedHandler(Handler<Set<TopicPartition>> handler) {
    this.stream.partitionsAssignedHandler(Helper.adaptHandler(handler));
    return this;
  }

  @Override
  public void commit() {
    this.stream.commit();
  }

  @Override
  public void commit(Handler<AsyncResult<Void>> completionHandler) {
    this.stream.commit(completionHandler != null ? ar -> ar.map((Object) null) : null);
  }

  @Override
  public void close(Handler<Void> completionHandler) {
    this.stream.close(completionHandler);
  }

  @Override
  public KafkaReadStream<K, V> asStream() {
    return this.stream;
  }
}
