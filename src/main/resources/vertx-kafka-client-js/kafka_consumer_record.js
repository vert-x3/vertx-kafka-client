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

/** @module vertx-kafka-client-js/kafka_consumer_record */
var utils = require('vertx-js/util/utils');

var io = Packages.io;
var JsonObject = io.vertx.core.json.JsonObject;
var JKafkaConsumerRecord = Java.type('io.vertx.kafka.client.consumer.KafkaConsumerRecord');

/**
 Vert.x Kafka consumer record

 @class
*/
var KafkaConsumerRecord = function(j_val, j_arg_0, j_arg_1) {

  var j_kafkaConsumerRecord = j_val;
  var that = this;
  var j_K = typeof j_arg_0 !== 'undefined' ? j_arg_0 : utils.unknown_jtype;
  var j_V = typeof j_arg_1 !== 'undefined' ? j_arg_1 : utils.unknown_jtype;

  /**

   @public

   @return {string} the topic this record is received from
   */
  this.topic = function() {
    var __args = arguments;
    if (__args.length === 0) {
      return j_kafkaConsumerRecord["topic()"]();
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**

   @public

   @return {number} the partition from which this record is received
   */
  this.partition = function() {
    var __args = arguments;
    if (__args.length === 0) {
      return j_kafkaConsumerRecord["partition()"]();
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**

   @public

   @return {number} the position of this record in the corresponding Kafka partition.
   */
  this.offset = function() {
    var __args = arguments;
    if (__args.length === 0) {
      return j_kafkaConsumerRecord["offset()"]();
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**

   @public

   @return {number} the timestamp of this record
   */
  this.timestamp = function() {
    var __args = arguments;
    if (__args.length === 0) {
      return j_kafkaConsumerRecord["timestamp()"]();
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**

   @public

   @return {Object} the timestamp type of this record
   */
  this.timestampType = function() {
    var __args = arguments;
    if (__args.length === 0) {
      return utils.convReturnEnum(j_kafkaConsumerRecord["timestampType()"]());
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**

   @public

   @return {number} the checksum (CRC32) of the record.
   */
  this.checksum = function() {
    var __args = arguments;
    if (__args.length === 0) {
      return j_kafkaConsumerRecord["checksum()"]();
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**

   @public

   @return {Object} the key (or null if no key is specified)
   */
  this.key = function() {
    var __args = arguments;
    if (__args.length === 0) {
      return j_K.wrap(j_kafkaConsumerRecord["key()"]());
    } else throw new TypeError('function invoked with invalid arguments');
  };

  /**

   @public

   @return {Object} the value
   */
  this.value = function() {
    var __args = arguments;
    if (__args.length === 0) {
      return j_V.wrap(j_kafkaConsumerRecord["value()"]());
    } else throw new TypeError('function invoked with invalid arguments');
  };

  // A reference to the underlying Java delegate
  // NOTE! This is an internal API and must not be used in user code.
  // If you rely on this property your code is likely to break if we change it / remove it without warning.
  this._jdel = j_kafkaConsumerRecord;
};

KafkaConsumerRecord._jclass = utils.getJavaClass("io.vertx.kafka.client.consumer.KafkaConsumerRecord");
KafkaConsumerRecord._jtype = {
  accept: function(obj) {
    return KafkaConsumerRecord._jclass.isInstance(obj._jdel);
  },
  wrap: function(jdel) {
    var obj = Object.create(KafkaConsumerRecord.prototype, {});
    KafkaConsumerRecord.apply(obj, arguments);
    return obj;
  },
  unwrap: function(obj) {
    return obj._jdel;
  }
};
KafkaConsumerRecord._create = function(jdel) {
  var obj = Object.create(KafkaConsumerRecord.prototype, {});
  KafkaConsumerRecord.apply(obj, arguments);
  return obj;
}
module.exports = KafkaConsumerRecord;