package io.vertx.kafka.client.producer.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.kafka.client.common.KafkaPartitionInfo;
import io.vertx.kafka.client.common.impl.KafkaPartitionInfoImpl;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.KafkaRecordMetadata;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import org.apache.kafka.common.PartitionInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:ppatierno@live.com">Paolo Patierno</a>
 */
public class KafkaProducerImpl<K, V> implements KafkaProducer<K, V> {

  private final KafkaWriteStream<K, V> stream;

  public KafkaProducerImpl(KafkaWriteStream<K, V> stream) {
    this.stream = stream;
  }

  @Override
  public KafkaProducer<K, V> exceptionHandler(Handler<Throwable> handler) {
    this.stream.exceptionHandler(handler);
    return this;
  }

  @Override
  public KafkaProducer<K, V> write(KafkaProducerRecord<K, V> kafkaProducerRecord) {
    this.stream.write(kafkaProducerRecord.record());
    return this;
  }

  @Override
  public KafkaProducer<K, V> write(KafkaProducerRecord<K, V> kafkaProducerRecord, Handler<KafkaRecordMetadata> handler) {
    this.stream.write(kafkaProducerRecord.record(), metadata -> {
      handler.handle(new KafkaRecordMetadataImpl(metadata));
    });
    return this;
  }

  @Override
  public KafkaProducer<K, V> partitionsFor(String topic, Handler<AsyncResult<List<KafkaPartitionInfo>>> handler) {
    this.stream.partitionsFor(topic, done -> {

      if (done.succeeded()) {
        List<KafkaPartitionInfo> partitions = new ArrayList<>();
        for (PartitionInfo partition: done.result()) {
          partitions.add(new KafkaPartitionInfoImpl(partition));
        }
        handler.handle(Future.succeededFuture(partitions));
      } else {
        handler.handle(Future.failedFuture(done.cause()));
      }

    });
    return this;
  }

  @Override
  public void end() {
    this.stream.end();
  }

  @Override
  public void end(KafkaProducerRecord<K, V> kafkaProducerRecord) {
    this.stream.end(kafkaProducerRecord.record());
  }

  @Override
  public KafkaProducer<K, V> setWriteQueueMaxSize(int size) {
    this.stream.setWriteQueueMaxSize(size);
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return this.stream.writeQueueFull();
  }

  @Override
  public KafkaProducer<K, V> drainHandler(Handler<Void> handler) {
    this.stream.drainHandler(handler);
    return this;
  }

  @Override
  public KafkaProducer<K, V> flush(Handler<Void> completionHandler) {
    this.stream.flush(completionHandler);
    return this;
  }

  @Override
  public void close() {
    this.stream.close();
  }

  @Override
  public void close(long timeout, Handler<Void> completionHandler) {
    this.stream.close(timeout, completionHandler);
  }

  @Override
  public KafkaWriteStream<K, V> asStream() {
    return this.stream;
  }
}
