/*
 * Copyright 2014 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

/** @module vertx-kafka-client-js/kafka_consumer */
var utils = require('vertx-js/util/utils');
var KafkaConsumerRecord = require('vertx-kafka-client-js/kafka_consumer_record');
var Vertx = require('vertx-js/vertx');
var ReadStream = require('vertx-js/read_stream');

var io = Packages.io;
var JsonObject = io.vertx.core.json.JsonObject;
var JKafkaConsumer = Java.type('io.vertx.kafka.client.consumer.KafkaConsumer');
var OffsetAndMetadata = Java.type('io.vertx.kafka.client.consumer.OffsetAndMetadata');
var PartitionInfo = Java.type('io.vertx.kafka.client.common.PartitionInfo');
var TopicPartition = Java.type('io.vertx.kafka.client.common.TopicPartition');

/**
 Vert.x Kafka consumer

 @class
*/
var KafkaConsumer = function(j_val, j_arg_0, j_arg_1) {

  var j_kafkaConsumer = j_val;
  var that = this;
  var j_K = typeof j_arg_0 !== 'undefined' ? j_arg_0 : utils.unknown_jtype;
  var j_V = typeof j_arg_1 !== 'undefined' ? j_arg_1 : utils.unknown_jtype;
  ReadStream.call(this, j_val);

  /**

   @public
   @param handler {function} 
   @return {KafkaConsumer}
   */
  this.exceptionHandler = function(handler) {
    var __args = arguments;
    if (__args.length === 1 && typeof __args[0] === 'function') {
      j_kafkaConsumer["exceptionHandler(io.vertx.core.Handler)"](function(jVal) {
      handler(utils.convReturnThrowable(jVal));
    });
      return that;
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**

   @public
   @param handler {function} 
   @return {KafkaConsumer}
   */
  this.handler = function(handler) {
    var __args = arguments;
    if (__args.length === 1 && typeof __args[0] === 'function') {
      j_kafkaConsumer["handler(io.vertx.core.Handler)"](function(jVal) {
      handler(utils.convReturnVertxGen(KafkaConsumerRecord, jVal, undefined, undefined));
    });
      return that;
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**
   Suspend fetching from the requested partitions.

   @public
   @param topicPartitions {Array.<Object>} topic partition from which suspend fetching 
   @param completionHandler {function} handler called on operation completed 
   @return {KafkaConsumer} current KafkaConsumer instance
   */
  this.pause = function() {
    var __args = arguments;
    if (__args.length === 0) {
      j_kafkaConsumer["pause()"]();
      return that;
    }  else if (__args.length === 1 && (typeof __args[0] === 'object' && __args[0] != null)) {
      j_kafkaConsumer["pause(io.vertx.kafka.client.common.TopicPartition)"](__args[0] != null ? new TopicPartition(new JsonObject(Java.asJSONCompatible(__args[0]))) : null);
      return that;
    }  else if (__args.length === 1 && typeof __args[0] === 'object' && __args[0] instanceof Array) {
      j_kafkaConsumer["pause(java.util.Set)"](utils.convParamSetDataObject(__args[0], function(json) { return new TopicPartition(json); }));
      return that;
    }  else if (__args.length === 2 && (typeof __args[0] === 'object' && __args[0] != null) && typeof __args[1] === 'function') {
      j_kafkaConsumer["pause(io.vertx.kafka.client.common.TopicPartition,io.vertx.core.Handler)"](__args[0] != null ? new TopicPartition(new JsonObject(Java.asJSONCompatible(__args[0]))) : null, function(ar) {
      if (ar.succeeded()) {
        __args[1](null, null);
      } else {
        __args[1](null, ar.cause());
      }
    });
      return that;
    }  else if (__args.length === 2 && typeof __args[0] === 'object' && __args[0] instanceof Array && typeof __args[1] === 'function') {
      j_kafkaConsumer["pause(java.util.Set,io.vertx.core.Handler)"](utils.convParamSetDataObject(__args[0], function(json) { return new TopicPartition(json); }), function(ar) {
      if (ar.succeeded()) {
        __args[1](null, null);
      } else {
        __args[1](null, ar.cause());
      }
    });
      return that;
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**
   Resume specified partitions which have been paused with pause.

   @public
   @param topicPartitions {Array.<Object>} topic partition from which resume fetching 
   @param completionHandler {function} handler called on operation completed 
   @return {KafkaConsumer} current KafkaConsumer instance
   */
  this.resume = function() {
    var __args = arguments;
    if (__args.length === 0) {
      j_kafkaConsumer["resume()"]();
      return that;
    }  else if (__args.length === 1 && (typeof __args[0] === 'object' && __args[0] != null)) {
      j_kafkaConsumer["resume(io.vertx.kafka.client.common.TopicPartition)"](__args[0] != null ? new TopicPartition(new JsonObject(Java.asJSONCompatible(__args[0]))) : null);
      return that;
    }  else if (__args.length === 1 && typeof __args[0] === 'object' && __args[0] instanceof Array) {
      j_kafkaConsumer["resume(java.util.Set)"](utils.convParamSetDataObject(__args[0], function(json) { return new TopicPartition(json); }));
      return that;
    }  else if (__args.length === 2 && (typeof __args[0] === 'object' && __args[0] != null) && typeof __args[1] === 'function') {
      j_kafkaConsumer["resume(io.vertx.kafka.client.common.TopicPartition,io.vertx.core.Handler)"](__args[0] != null ? new TopicPartition(new JsonObject(Java.asJSONCompatible(__args[0]))) : null, function(ar) {
      if (ar.succeeded()) {
        __args[1](null, null);
      } else {
        __args[1](null, ar.cause());
      }
    });
      return that;
    }  else if (__args.length === 2 && typeof __args[0] === 'object' && __args[0] instanceof Array && typeof __args[1] === 'function') {
      j_kafkaConsumer["resume(java.util.Set,io.vertx.core.Handler)"](utils.convParamSetDataObject(__args[0], function(json) { return new TopicPartition(json); }), function(ar) {
      if (ar.succeeded()) {
        __args[1](null, null);
      } else {
        __args[1](null, ar.cause());
      }
    });
      return that;
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**

   @public
   @param endHandler {function} 
   @return {KafkaConsumer}
   */
  this.endHandler = function(endHandler) {
    var __args = arguments;
    if (__args.length === 1 && typeof __args[0] === 'function') {
      j_kafkaConsumer["endHandler(io.vertx.core.Handler)"](endHandler);
      return that;
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**
   Subscribe to the given list of topics to get dynamically assigned partitions.

   @public
   @param topics {Array.<string>} topics to subscribe to 
   @param completionHandler {function} handler called on operation completed 
   @return {KafkaConsumer} current KafkaConsumer instance
   */
  this.subscribe = function() {
    var __args = arguments;
    if (__args.length === 1 && typeof __args[0] === 'string') {
      j_kafkaConsumer["subscribe(java.lang.String)"](__args[0]);
      return that;
    }  else if (__args.length === 1 && typeof __args[0] === 'object' && __args[0] instanceof Array) {
      j_kafkaConsumer["subscribe(java.util.Set)"](utils.convParamSetBasicOther(__args[0]));
      return that;
    }  else if (__args.length === 2 && typeof __args[0] === 'string' && typeof __args[1] === 'function') {
      j_kafkaConsumer["subscribe(java.lang.String,io.vertx.core.Handler)"](__args[0], function(ar) {
      if (ar.succeeded()) {
        __args[1](null, null);
      } else {
        __args[1](null, ar.cause());
      }
    });
      return that;
    }  else if (__args.length === 2 && typeof __args[0] === 'object' && __args[0] instanceof Array && typeof __args[1] === 'function') {
      j_kafkaConsumer["subscribe(java.util.Set,io.vertx.core.Handler)"](utils.convParamSetBasicOther(__args[0]), function(ar) {
      if (ar.succeeded()) {
        __args[1](null, null);
      } else {
        __args[1](null, ar.cause());
      }
    });
      return that;
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**
   Manually assign a list of partition to this consumer.

   @public
   @param topicPartitions {Array.<Object>} partitions which want assigned 
   @param completionHandler {function} handler called on operation completed 
   @return {KafkaConsumer} current KafkaConsumer instance
   */
  this.assign = function() {
    var __args = arguments;
    if (__args.length === 1 && (typeof __args[0] === 'object' && __args[0] != null)) {
      j_kafkaConsumer["assign(io.vertx.kafka.client.common.TopicPartition)"](__args[0] != null ? new TopicPartition(new JsonObject(Java.asJSONCompatible(__args[0]))) : null);
      return that;
    }  else if (__args.length === 1 && typeof __args[0] === 'object' && __args[0] instanceof Array) {
      j_kafkaConsumer["assign(java.util.Set)"](utils.convParamSetDataObject(__args[0], function(json) { return new TopicPartition(json); }));
      return that;
    }  else if (__args.length === 2 && (typeof __args[0] === 'object' && __args[0] != null) && typeof __args[1] === 'function') {
      j_kafkaConsumer["assign(io.vertx.kafka.client.common.TopicPartition,io.vertx.core.Handler)"](__args[0] != null ? new TopicPartition(new JsonObject(Java.asJSONCompatible(__args[0]))) : null, function(ar) {
      if (ar.succeeded()) {
        __args[1](null, null);
      } else {
        __args[1](null, ar.cause());
      }
    });
      return that;
    }  else if (__args.length === 2 && typeof __args[0] === 'object' && __args[0] instanceof Array && typeof __args[1] === 'function') {
      j_kafkaConsumer["assign(java.util.Set,io.vertx.core.Handler)"](utils.convParamSetDataObject(__args[0], function(json) { return new TopicPartition(json); }), function(ar) {
      if (ar.succeeded()) {
        __args[1](null, null);
      } else {
        __args[1](null, ar.cause());
      }
    });
      return that;
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**
   Get the set of partitions currently assigned to this consumer.

   @public
   @param handler {function} handler called on operation completed 
   @return {KafkaConsumer} current KafkaConsumer instance
   */
  this.assignment = function(handler) {
    var __args = arguments;
    if (__args.length === 1 && typeof __args[0] === 'function') {
      j_kafkaConsumer["assignment(io.vertx.core.Handler)"](function(ar) {
      if (ar.succeeded()) {
        handler(utils.convReturnListSetDataObject(ar.result()), null);
      } else {
        handler(null, ar.cause());
      }
    });
      return that;
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**
   Unsubscribe from topics currently subscribed with subscribe.

   @public
   @param completionHandler {function} handler called on operation completed 
   @return {KafkaConsumer} current KafkaConsumer instance
   */
  this.unsubscribe = function() {
    var __args = arguments;
    if (__args.length === 0) {
      j_kafkaConsumer["unsubscribe()"]();
      return that;
    }  else if (__args.length === 1 && typeof __args[0] === 'function') {
      j_kafkaConsumer["unsubscribe(io.vertx.core.Handler)"](function(ar) {
      if (ar.succeeded()) {
        __args[0](null, null);
      } else {
        __args[0](null, ar.cause());
      }
    });
      return that;
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**
   Get the current subscription.

   @public
   @param handler {function} handler called on operation completed 
   @return {KafkaConsumer} current KafkaConsumer instance
   */
  this.subscription = function(handler) {
    var __args = arguments;
    if (__args.length === 1 && typeof __args[0] === 'function') {
      j_kafkaConsumer["subscription(io.vertx.core.Handler)"](function(ar) {
      if (ar.succeeded()) {
        handler(utils.convReturnSet(ar.result()), null);
      } else {
        handler(null, ar.cause());
      }
    });
      return that;
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**
   Get the set of partitions that were previously paused by a call to pause(Set).

   @public
   @param handler {function} handler called on operation completed 
   */
  this.paused = function(handler) {
    var __args = arguments;
    if (__args.length === 1 && typeof __args[0] === 'function') {
      j_kafkaConsumer["paused(io.vertx.core.Handler)"](function(ar) {
      if (ar.succeeded()) {
        handler(utils.convReturnListSetDataObject(ar.result()), null);
      } else {
        handler(null, ar.cause());
      }
    });
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**
   Set the handler called when topic partitions are revoked to the consumer

   @public
   @param handler {function} handler called on revoked topic partitions 
   @return {KafkaConsumer} current KafkaConsumer instance
   */
  this.partitionsRevokedHandler = function(handler) {
    var __args = arguments;
    if (__args.length === 1 && typeof __args[0] === 'function') {
      j_kafkaConsumer["partitionsRevokedHandler(io.vertx.core.Handler)"](function(jVal) {
      handler(utils.convReturnListSetDataObject(jVal));
    });
      return that;
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**
   Set the handler called when topic partitions are assigned to the consumer

   @public
   @param handler {function} handler called on assigned topic partitions 
   @return {KafkaConsumer} current KafkaConsumer instance
   */
  this.partitionsAssignedHandler = function(handler) {
    var __args = arguments;
    if (__args.length === 1 && typeof __args[0] === 'function') {
      j_kafkaConsumer["partitionsAssignedHandler(io.vertx.core.Handler)"](function(jVal) {
      handler(utils.convReturnListSetDataObject(jVal));
    });
      return that;
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**
   Overrides the fetch offsets that the consumer will use on the next poll.

   @public
   @param topicPartition {Object} topic partition for which seek 
   @param offset {number} offset to seek inside the topic partition 
   @param completionHandler {function} handler called on operation completed 
   @return {KafkaConsumer} current KafkaConsumer instance
   */
  this.seek = function() {
    var __args = arguments;
    if (__args.length === 2 && (typeof __args[0] === 'object' && __args[0] != null) && typeof __args[1] ==='number') {
      j_kafkaConsumer["seek(io.vertx.kafka.client.common.TopicPartition,long)"](__args[0] != null ? new TopicPartition(new JsonObject(Java.asJSONCompatible(__args[0]))) : null, __args[1]);
      return that;
    }  else if (__args.length === 3 && (typeof __args[0] === 'object' && __args[0] != null) && typeof __args[1] ==='number' && typeof __args[2] === 'function') {
      j_kafkaConsumer["seek(io.vertx.kafka.client.common.TopicPartition,long,io.vertx.core.Handler)"](__args[0] != null ? new TopicPartition(new JsonObject(Java.asJSONCompatible(__args[0]))) : null, __args[1], function(ar) {
      if (ar.succeeded()) {
        __args[2](null, null);
      } else {
        __args[2](null, ar.cause());
      }
    });
      return that;
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**
   Seek to the first offset for each of the given partitions.

   @public
   @param topicPartitions {Array.<Object>} topic partition for which seek 
   @param completionHandler {function} handler called on operation completed 
   @return {KafkaConsumer} current KafkaConsumer instance
   */
  this.seekToBeginning = function() {
    var __args = arguments;
    if (__args.length === 1 && (typeof __args[0] === 'object' && __args[0] != null)) {
      j_kafkaConsumer["seekToBeginning(io.vertx.kafka.client.common.TopicPartition)"](__args[0] != null ? new TopicPartition(new JsonObject(Java.asJSONCompatible(__args[0]))) : null);
      return that;
    }  else if (__args.length === 1 && typeof __args[0] === 'object' && __args[0] instanceof Array) {
      j_kafkaConsumer["seekToBeginning(java.util.Set)"](utils.convParamSetDataObject(__args[0], function(json) { return new TopicPartition(json); }));
      return that;
    }  else if (__args.length === 2 && (typeof __args[0] === 'object' && __args[0] != null) && typeof __args[1] === 'function') {
      j_kafkaConsumer["seekToBeginning(io.vertx.kafka.client.common.TopicPartition,io.vertx.core.Handler)"](__args[0] != null ? new TopicPartition(new JsonObject(Java.asJSONCompatible(__args[0]))) : null, function(ar) {
      if (ar.succeeded()) {
        __args[1](null, null);
      } else {
        __args[1](null, ar.cause());
      }
    });
      return that;
    }  else if (__args.length === 2 && typeof __args[0] === 'object' && __args[0] instanceof Array && typeof __args[1] === 'function') {
      j_kafkaConsumer["seekToBeginning(java.util.Set,io.vertx.core.Handler)"](utils.convParamSetDataObject(__args[0], function(json) { return new TopicPartition(json); }), function(ar) {
      if (ar.succeeded()) {
        __args[1](null, null);
      } else {
        __args[1](null, ar.cause());
      }
    });
      return that;
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**
   Seek to the last offset for each of the given partitions.

   @public
   @param topicPartitions {Array.<Object>} topic partition for which seek 
   @param completionHandler {function} handler called on operation completed 
   @return {KafkaConsumer} current KafkaConsumer instance
   */
  this.seekToEnd = function() {
    var __args = arguments;
    if (__args.length === 1 && (typeof __args[0] === 'object' && __args[0] != null)) {
      j_kafkaConsumer["seekToEnd(io.vertx.kafka.client.common.TopicPartition)"](__args[0] != null ? new TopicPartition(new JsonObject(Java.asJSONCompatible(__args[0]))) : null);
      return that;
    }  else if (__args.length === 1 && typeof __args[0] === 'object' && __args[0] instanceof Array) {
      j_kafkaConsumer["seekToEnd(java.util.Set)"](utils.convParamSetDataObject(__args[0], function(json) { return new TopicPartition(json); }));
      return that;
    }  else if (__args.length === 2 && (typeof __args[0] === 'object' && __args[0] != null) && typeof __args[1] === 'function') {
      j_kafkaConsumer["seekToEnd(io.vertx.kafka.client.common.TopicPartition,io.vertx.core.Handler)"](__args[0] != null ? new TopicPartition(new JsonObject(Java.asJSONCompatible(__args[0]))) : null, function(ar) {
      if (ar.succeeded()) {
        __args[1](null, null);
      } else {
        __args[1](null, ar.cause());
      }
    });
      return that;
    }  else if (__args.length === 2 && typeof __args[0] === 'object' && __args[0] instanceof Array && typeof __args[1] === 'function') {
      j_kafkaConsumer["seekToEnd(java.util.Set,io.vertx.core.Handler)"](utils.convParamSetDataObject(__args[0], function(json) { return new TopicPartition(json); }), function(ar) {
      if (ar.succeeded()) {
        __args[1](null, null);
      } else {
        __args[1](null, ar.cause());
      }
    });
      return that;
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**
   Commit current offsets for all the subscribed list of topics and partition.

   @public
   @param completionHandler {function} handler called on operation completed 
   */
  this.commit = function() {
    var __args = arguments;
    if (__args.length === 0) {
      j_kafkaConsumer["commit()"]();
    }  else if (__args.length === 1 && typeof __args[0] === 'function') {
      j_kafkaConsumer["commit(io.vertx.core.Handler)"](function(ar) {
      if (ar.succeeded()) {
        __args[0](null, null);
      } else {
        __args[0](null, ar.cause());
      }
    });
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**
   Get the last committed offset for the given partition (whether the commit happened by this process or another).

   @public
   @param topicPartition {Object} topic partition for getting last committed offset 
   @param handler {function} handler called on operation completed 
   */
  this.committed = function(topicPartition, handler) {
    var __args = arguments;
    if (__args.length === 2 && (typeof __args[0] === 'object' && __args[0] != null) && typeof __args[1] === 'function') {
      j_kafkaConsumer["committed(io.vertx.kafka.client.common.TopicPartition,io.vertx.core.Handler)"](topicPartition != null ? new TopicPartition(new JsonObject(Java.asJSONCompatible(topicPartition))) : null, function(ar) {
      if (ar.succeeded()) {
        handler(utils.convReturnDataObject(ar.result()), null);
      } else {
        handler(null, ar.cause());
      }
    });
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**
   Get metadata about the partitions for a given topic.

   @public
   @param topic {string} topic partition for which getting partitions info 
   @param handler {function} handler called on operation completed 
   @return {KafkaConsumer} current KafkaConsumer instance
   */
  this.partitionsFor = function(topic, handler) {
    var __args = arguments;
    if (__args.length === 2 && typeof __args[0] === 'string' && typeof __args[1] === 'function') {
      j_kafkaConsumer["partitionsFor(java.lang.String,io.vertx.core.Handler)"](topic, function(ar) {
      if (ar.succeeded()) {
        handler(utils.convReturnListSetDataObject(ar.result()), null);
      } else {
        handler(null, ar.cause());
      }
    });
      return that;
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**
   Close the consumer

   @public
   @param completionHandler {function} handler called on operation completed 
   */
  this.close = function() {
    var __args = arguments;
    if (__args.length === 0) {
      j_kafkaConsumer["close()"]();
    }  else if (__args.length === 1 && typeof __args[0] === 'function') {
      j_kafkaConsumer["close(io.vertx.core.Handler)"](__args[0]);
    } else throw new TypeError('function invoked with invalid arguments');
  };

  // A reference to the underlying Java delegate
  // NOTE! This is an internal API and must not be used in user code.
  // If you rely on this property your code is likely to break if we change it / remove it without warning.
  this._jdel = j_kafkaConsumer;
};

KafkaConsumer._jclass = utils.getJavaClass("io.vertx.kafka.client.consumer.KafkaConsumer");
KafkaConsumer._jtype = {
  accept: function(obj) {
    return KafkaConsumer._jclass.isInstance(obj._jdel);
  },
  wrap: function(jdel) {
    var obj = Object.create(KafkaConsumer.prototype, {});
    KafkaConsumer.apply(obj, arguments);
    return obj;
  },
  unwrap: function(obj) {
    return obj._jdel;
  }
};
KafkaConsumer._create = function(jdel) {
  var obj = Object.create(KafkaConsumer.prototype, {});
  KafkaConsumer.apply(obj, arguments);
  return obj;
}
/**
 Create a new KafkaConsumer instance

 @memberof module:vertx-kafka-client-js/kafka_consumer
 @param vertx {Vertx} Vert.x instance to use 
 @param config {Array.<string>} Kafka consumer configuration 
 @param keyType {todo} class type for the key deserialization 
 @param valueType {todo} class type for the value deserialization 
 @return {KafkaConsumer} an instance of the KafkaConsumer
 */
KafkaConsumer.create = function() {
  var __args = arguments;
  if (__args.length === 2 && typeof __args[0] === 'object' && __args[0]._jdel && (typeof __args[1] === 'object' && __args[1] != null)) {
    return utils.convReturnVertxGen(KafkaConsumer, JKafkaConsumer["create(io.vertx.core.Vertx,java.util.Map)"](__args[0]._jdel, __args[1]), undefined, undefined);
  }else if (__args.length === 4 && typeof __args[0] === 'object' && __args[0]._jdel && (typeof __args[1] === 'object' && __args[1] != null) && typeof __args[2] === 'function' && typeof __args[3] === 'function') {
    return utils.convReturnVertxGen(KafkaConsumer, JKafkaConsumer["create(io.vertx.core.Vertx,java.util.Map,java.lang.Class,java.lang.Class)"](__args[0]._jdel, __args[1], utils.get_jclass(__args[2]), utils.get_jclass(__args[3])), utils.get_jtype(__args[2]), utils.get_jtype(__args[3]));
  } else throw new TypeError('function invoked with invalid arguments');
};

module.exports = KafkaConsumer;