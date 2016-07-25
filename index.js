/**
 * Created by lanxingcan on 16/7/25.
 */

const Redis = require('ioredis');
const co = require('co');
const assert = require('assert');

const table = new Map();

/**
 * Starting a watchdog instance
 *
 * @param watchdogRedis Instance of IORedis that will enter subscribe mode, can pass null and then create it internally
 * @param watchdogRedisHost Host for internal watchdog IORedis instance
 * @param watchdogRedisPort Port for internal watchdog IORedis instance
 * @param watchdogRedisSpace Database index for internal watchdog IORedis instance
 * @param redis Instance of IORedis that will manipulate the queue, can pass null and then create it internally.
 * @param redisHost Host for internal IORedis instance
 * @param redisPort Port for internal IORedis instance
 * @param redisSpace Database index for internal IORedis instance
 * @param watchdogTimeout Timeout of the watchdog
 * @param watchdogTopic Topic name which watchdog is subscribe to
 * @param setTimeout setTimeout function, pass a stub if you want to do test.
 */
module.exports = function({
    watchdogRedis,
    watchdogRedisHost = 'localhost', watchdogRedisPort = 6379, watchdogRedisSpace = 0,
    redis,
    redisHost = 'localhost', redisPort = 6379, redisSpace = 0, watchdogTimeout = 30000, watchdogTopic,
    setTimeout = setTimeout
}) {

    "use strict";
    if (watchdogTopic == null || watchdogTopic == '') throw new TypeError('watchdogTopic invalid');

    redis = redis || new Redis({host: redisHost, port: redisPort, db: redisSpace});
    assert(redis instanceof Redis);

    watchdogRedis = watchdogRedis || new Redis({host: watchdogRedisHost, port: watchdogRedisPort, db: watchdogRedisSpace});
    assert(watchdogRedis instanceof Redis);

    watchdogRedis.on('message', (topic, data) => {
        if (topic == watchdogTopic) {

            try {
                let { sponge, queue } = JSON.parse(data);
                let timeoutHandle = table.get(sponge);
                table.delete(timeoutHandle);
                if (timeoutHandle)
                    clearTimeout(timeoutHandle);

                table.set(sponge, setTimeout(()=> {
                    co(function *(){

                        // Prevent leak!
                        table.delete(sponge);

                        // Move elements from sponge back to the queue
                        while(null != yield redis.rpoplpush(sponge, queue).exec()) {

                        }

                    }).catch((e)=> console.error(e));
                }, watchdogTimeout));
            }
        }
    });
};