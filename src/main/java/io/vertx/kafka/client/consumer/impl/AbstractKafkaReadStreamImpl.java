package io.vertx.kafka.client.consumer.impl;

import io.vertx.core.*;
import io.vertx.core.internal.ContextInternal;
import io.vertx.kafka.client.common.KafkaClientOptions;
import io.vertx.kafka.client.common.tracing.ConsumerTracer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract/common worker-thread class used by {@link KafkaShareReadStreamImpl}
 * and maybe in the future by {@link KafkaReadStreamImpl} for: demand tracking, the schedule/run loop,
 * and the single-threaded executor lifecycle.
 * <p>
 * Subclasses must implement the following method:
 * <ul>
 *   <li>{@link #pollRecords(Handler)} — perform a single poll against the broker on the
 *       worker thread and deliver the result to the provided handler on the event loop</li>
 * </ul>
 * Close and wakeup behavior is provided by passing {@code Runnable} references to the
 * constructor, so subclasses do not need to override anything for lifecycle management.
 */
abstract class AbstractKafkaReadStreamImpl<K, V> {

  private static final AtomicInteger threadCount = new AtomicInteger(0);

  protected final Context context;
  private final Runnable wakeup;
  private final Runnable closeNative;
  private final ConsumerTracer tracer;

  protected final AtomicBoolean closed = new AtomicBoolean(true);
  protected final AtomicBoolean polling = new AtomicBoolean(false);
  protected final AtomicLong demand = new AtomicLong(Long.MAX_VALUE);

  protected ExecutorService worker;
  protected Handler<ConsumerRecord<K, V>> recordHandler;
  protected Handler<ConsumerRecords<K, V>> batchHandler;
  protected Handler<Throwable> exceptionHandler;
  protected Handler<Void> endHandler;
  protected Duration pollTimeout = Duration.ofSeconds(1);
  private Iterator<ConsumerRecord<K, V>> current;

  protected AbstractKafkaReadStreamImpl(Vertx vertx, Runnable wakeup, Runnable closeNative, KafkaClientOptions options) {
    ContextInternal ctxInt = ((ContextInternal) vertx.getOrCreateContext()).unwrap();
    this.context = ctxInt;
    this.wakeup = wakeup;
    this.closeNative = closeNative;
    this.tracer = ConsumerTracer.create(ctxInt.tracer(), options);
  }

  protected ExecutorService createWorker(String namePrefix) {
    return Executors.newSingleThreadExecutor(r -> new Thread(r, namePrefix + threadCount.getAndIncrement()));
  }

  public void exceptionHandler(Handler<Throwable> handler) {
    this.exceptionHandler = handler;
  }

  public void handler(Handler<ConsumerRecord<K, V>> handler) {
    this.recordHandler = handler;
    schedule();
  }

  public void batchHandler(Handler<ConsumerRecords<K, V>> handler) {
    this.batchHandler = handler;
    schedule();
  }

  public void endHandler(Handler<Void> handler) {
    this.endHandler = handler;
  }

  public void pause() {
    demand.set(0L);
  }

  public void resume() {
    fetch(Long.MAX_VALUE);
  }

  public void fetch(long amount) {
    if (amount < 0) {
      throw new IllegalArgumentException("Invalid claim " + amount);
    }
    long updated = demand.updateAndGet(d -> {
      if (d == Long.MAX_VALUE) return d;
      long sum = d + amount;
      return sum < 0L ? Long.MAX_VALUE : sum;
    });
    if (updated > 0L) {
      schedule();
    }
  }

  public Future<Void> close() {
    if (closed.compareAndSet(false, true)) {
      wakeup.run();
      Promise<Void> promise = ((ContextInternal) context).promise();
      worker.submit(() -> {
        try {
          closeNative.run();
          context.runOnContext(v -> {
            if (endHandler != null) endHandler.handle(null);
            promise.complete();
          });
        } catch (Exception e) {
          context.runOnContext(v -> promise.fail(e));
        }
      });
      return promise.future().onComplete(v -> worker.shutdownNow());
    }
    return ((ContextInternal) context).succeededFuture();
  }

  protected void schedule() {
    if (!closed.get() && demand.get() > 0L && (recordHandler != null || batchHandler != null)) {
      context.runOnContext(v -> run());
    }
  }

  private void run() {
    if (closed.get()) {
      return;
    }

    if (current == null || !current.hasNext()) {
      pollRecords(records -> {
        if (records != null && records.count() > 0) {
          if (batchHandler != null) {
            batchHandler.handle(records);
            schedule();
          } else {
            current = records.iterator();
            schedule();
          }
        } else {
          context.owner().setTimer(1, t -> schedule());
        }
      });
    } else {
      int count = 0;
      out:
      while (current.hasNext() && count++ < 10) {

        while (true) {
          long d = demand.get();
          if (d <= 0L) {
            break out;
          } else if (d == Long.MAX_VALUE || demand.compareAndSet(d, d - 1)) {
            break;
          }
        }

        ConsumerRecord<K, V> next = current.next();
        ContextInternal ctx = ((ContextInternal) context).duplicate();
        ctx.emit(v -> tracedHandler(ctx, recordHandler).handle(next));
      }
      schedule();
    }
  }

  private Handler<ConsumerRecord<K, V>> tracedHandler(Context ctx, Handler<ConsumerRecord<K, V>> handler) {
    return tracer == null ? handler : rec -> {
      ConsumerTracer.StartedSpan span = tracer.prepareMessageReceived(ctx, rec);
      try {
        handler.handle(rec);
        span.finish(ctx);
      } catch (Throwable t) {
        span.fail(ctx, t);
        throw t;
      }
    };
  }

  /**
   * Poll a batch of records from the broker on the worker thread and deliver
   * the result by calling {@code handler} on the event loop.
   */
  protected abstract void pollRecords(Handler<ConsumerRecords<K, V>> handler);
}
