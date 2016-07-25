# RRMQ 
Redis Reliable Message Queue

[![Build Status](https://travis-ci.org/thynson/rrmq.svg?branch=master)](https://travis-ci.org/thynson/rrmq)
[![npm version](https://badge.fury.io/js/rrmq.svg)](https://badge.fury.io/js/rrmq)

## Introduction
  This library implements a message queue over redis based on feature of 
  `BRPOPLPUSH` and `PUBLISH`/`SUBSCRIBE`.
  
  There are three role in this system:
  
  * Producer
  
    An instance of producer may push message to queue
    
  * Consumer
  
    An instance of consumer await and pop message from the queue and 
    meanwhile keeps heartbeat via publish message to a specified channel
    with a per-instance unique identifier to inform the watchdog that 
    this instance is alive. 
    
    If an instance died while processing a message. Watchdogs will 
    notice that an instance has timed out and the message will be push
    back to the queue.
  
  * Watchdog
 
    A watchdog subscribe a specified channel to watch the status of 
    consumers.
    
## Example

* `watchdog.js`

```javascript
const { RedisQueueWatchdog } = require('rrmq');
new RedisQueueWatchdog({
  watchdogRedisHost: 'localhost', watchdogRedisPort: 6379,
  redisHost: 'localhost', redisPort:6379,
  watchdogTopic: 'test-watchdog'
})
.on('error', console.error)
.start();
```
 
* `consumer.js`

```javascript
const {RedisQueueConsumer} = require('rrmq');
new RedisQueueConsumer({
  watchdogRedisHost: 'localhost', watchdogRedisPort: 6379,
  redisHost: 'localhost', redisPort:6379,
  watchdogTopic: 'test-watchdog', queue: 'test-queue'
}).on('error', console.error)
.start(function(message) {
  return new Promise((done, fail)=> {
    // ...
  });
});
```

* `producer.js`

```javascript
const {RedisQueueProducer} = require('rrmq');
new RedisQueueConsumer({
  redisHost: 'localhost', redisPort:6379,
  queue: 'test-queue'
}).on('error', console.error)
.send('hello world message');
```