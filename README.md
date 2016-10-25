# RRMQ 
Redis Reliable Message Queue

[![Build Status](https://travis-ci.org/thynson/rrmq.svg?branch=master)](https://travis-ci.org/thynson/rrmq)
[![Coverage Status](https://coveralls.io/repos/github/thynson/rrmq/badge.svg?branch=master)](https://coveralls.io/github/thynson/rrmq?branch=master)
[![npm version](https://badge.fury.io/js/rrmq.svg)](https://badge.fury.io/js/rrmq)

## Install
This library currently requires `node@>4.0.0` and `npm@>3.0.0`. If you're using `node@4.*`,
you probably have `npm@2.*` installed, then you need to run the following command first.

`npm install -g npm@3`

If you've setup you environment properly, run

`npm install rrmq bluebird tuid`

where `bluebird` and `tuid` are [peer dependencies] of this library

## Introduction
  This library implements a message queue over redis based on feature of 
  `BRPOPLPUSH` and `PUBLISH`/`SUBSCRIBE`, and is written in [Typescript].
  
  There are three role in this system:
  
  * Producer
  
    An instance of producer may push message to queue
    
  * Watchdog
 
    A watchdog subscribe a specified channel to watch the status of 
    consumers. If one consumer is down, the message was being processed
    by that consumer will be recovered.
    
  * Consumer
  
    An instance of consumer await and pop message from the queue and 
    meanwhile keeps heartbeat via publish message to a specified channel
    with a per-instance unique identifier to inform the watchdog that 
    this instance is alive. 
    
    If an instance died while processing a message. Watchdogs will 
    notice that an instance has timed out and the message will be push
    back to the queue.
  
    
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

# Use with typescript

The typing file of this library can is placed on `node_modules/rrmq/built/index.d.ts`. 


[Typescript]: https://www.typescriptlang.org/
[peer dependencies]: https://nodejs.org/uk/blog/npm/peer-dependencies