package io.vertx.kafka.client.producer.impl;

import io.vertx.core.Handler;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.KafkaWriteStream;

/**
 * @author <a href="mailto:ppatierno@live.com">Paolo Patierno</a>
 */
public class KafkaProducerImpl<K, V> implements KafkaProducer<K, V> {

  private final KafkaWriteStream<K, V> stream;

  public KafkaProducerImpl(KafkaWriteStream<K, V> stream) {
    this.stream = stream;
  }

  @Override
  public KafkaProducerImpl<K, V> exceptionHandler(Handler<Throwable> handler) {
    this.stream.exceptionHandler(handler);
    return this;
  }

  @Override
  public KafkaProducerImpl<K, V> write(KafkaProducerRecord<K, V> kafkaProducerRecord) {
    this.stream.write(kafkaProducerRecord.record());
    return this;
  }

  @Override
  public void end() {

  }

  @Override
  public void end(KafkaProducerRecord<K, V> kafkaProducerRecord) {

  }

  @Override
  public KafkaProducerImpl<K, V> setWriteQueueMaxSize(int i) {
    return null;
  }

  @Override
  public boolean writeQueueFull() {
    return false;
  }

  @Override
  public KafkaProducerImpl<K, V> drainHandler(Handler<Void> handler) {
    return null;
  }
}
