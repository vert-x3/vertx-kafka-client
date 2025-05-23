/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.vertx.kafka.client.common.impl;

import io.vertx.core.*;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.VertxInternal;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * An helper class for managing automatic clean-up in verticles.
 *
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class CloseHandler {

  private Closeable closeable;
  private Runnable closeableHookCleanup;
  private final BiConsumer<Long, Completable<Void>> close;

  public CloseHandler(BiConsumer<Long, Completable<Void>> close) {
    this.close = close;
  }

  public void registerCloseHook(VertxInternal vertx) {
    registerCloseHook(vertx::addCloseHook, vertx::removeCloseHook);
  }

  public void registerCloseHook(ContextInternal context) {
    registerCloseHook(context::addCloseHook, context::removeCloseHook);
  }

  private synchronized void registerCloseHook(Consumer<Closeable> addCloseHook, Consumer<Closeable> removeCloseHook) {
    if (closeable == null) {
      closeable = ar -> {
        synchronized (CloseHandler.this) {
          if (closeable == null) {
            ar.succeed();
            return;
          }
          closeable = null;
        }
        close.accept(0L, ar);
      };
      closeableHookCleanup = () -> {
        synchronized (CloseHandler.this) {
          if (closeable != null) {
            removeCloseHook.accept(closeable);
            closeable = null;
          }
        }
      };
      addCloseHook.accept(closeable);
    }
  }

  public synchronized void unregisterCloseHook() {
    if (closeableHookCleanup != null) {
      closeableHookCleanup.run();
    }
  }

  public void close() {
    unregisterCloseHook();
    close.accept(0L, (res, err) -> {});
  }

  public void close(Completable<Void> completionHandler) {
    unregisterCloseHook();
    close.accept(0L, completionHandler);
  }

  public void close(long timeout, Completable<Void> completionHandler) {
    unregisterCloseHook();
    close.accept(timeout, completionHandler);
  }
}
