/*
 * Copyright (C) 2016 LAN Xingcan
 * All right reserved
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

const Redis = require('ioredis');
const co = require('co');
const assert = require('assert');
const event = require('events');
const UUID = require('node-uuid');


module.exports.RedisQueueWatchdog = class RedisQueueWatchdog extends event.EventEmitter {

    /**
     * Creating a watchdog instance
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
     * @param clearTimeout clearTimeout function, pass a stub if you want to do test.
     */
    constructor({
        watchdogRedis, redis,
        watchdogRedisHost, watchdogRedisPort, watchdogRedisSpace,
        redisHost, redisPort, redisSpace, watchdogTimeout, watchdogTopic,
        setTimeout, clearTimeout
    }){
        super();
        this.table = {};
        if (watchdogTopic == null || watchdogTopic == '') throw new TypeError('watchdogTopic invalid');
        this.watchdogTopic = watchdogTopic;
        this.watchdogTimeout = watchdogTimeout || 30000;
        this.setTimeout = setTimeout || global.setTimeout;
        this.clearTimeout = clearTimeout || global.clearTimeout;

        this.redis = redis || new Redis({host: redisHost , port: redisPort, db: redisSpace});
        assert(this.redis instanceof Redis);

        this.watchdogRedis = watchdogRedis || new Redis({host: watchdogRedisHost, port: watchdogRedisPort, db: watchdogRedisSpace});
        assert(this.watchdogRedis instanceof Redis);

    }

    /**
     * Start the watchdog
     * @returns {Promise}
     */
    start() {

        this.watchdogRedis.on('message', (topic, data) => {
            if (topic == this.watchdogTopic) {

                try {
                    let { sponge, queue } = JSON.parse(data);
                    let timeoutHandle = this.table[sponge];
                    if (timeoutHandle) {
                        delete this.table[sponge];
                        this.clearTimeout(timeoutHandle);
                    }
                    timeoutHandle = this.setTimeout(()=> {
                        // Prevent leak!
                        delete this.table[sponge];
                        co(function *(){
                            while(1 == (yield this.redis.rpoplpush(sponge, queue))) {}
                        }.bind(this)).catch((e)=> { this.emit('error', e);});
                    }, this.watchdogTimeout);

                    this.table[sponge]= timeoutHandle;

                } catch(e) {
                    this.emit('error', e);
                }
            }
        });
        return this.watchdogRedis.subscribe(this.watchdogTopic);
    }

    /**
     * Stop the watchdog
     * @returns {Promise}
     */
    stop() {
        return this.watchdogRedis.unsubscribe(this.watchdogTopic);
    }


};

module.exports.RedisQueueProducer = class RedisQueueProducer extends event.EventEmitter {

    /**
     * Creating a watchdog instance
     *
     * @param redis Instance of IORedis that will manipulate the queue, can pass null and then create it internally.
     * @param redisHost Host for internal IORedis instance
     * @param redisPort Port for internal IORedis instance
     * @param redisSpace Database index for internal IORedis instance
     * @param queue The name of the queue in redis
     */
    constructor({
        redis,
        redisHost, redisPort, redisSpace, queue,
    }) {
        super();
        if (queue == null || queue == '') throw new TypeError('queue invalid');

        this.queue = queue;
        this.redis = redis || new Redis({host: redisHost , port: redisPort, db: redisSpace});
        assert(this.redis instanceof Redis);
    }

    /**
     * Send message
     * @param message The message
     * @returns {Promise}
     */
    send(message) {
        return this.redis.lpush(this.queue, message);
    }
}

module.exports.RedisQueueConsumer = class RedisQueueConsumer extends event.EventEmitter {

    /**
     * Creating a watchdog instance
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
     * @param queue The name of the queue in redis
     * @param setInterval setInterval function, pass a stub if you want to do test.
     * @param clearInterval clearInterval function, pass a stub if you want to do test.
     */
    constructor({
        watchdogRedis, redis,
        watchdogRedisHost, watchdogRedisPort, watchdogRedisSpace,
        redisHost, redisPort, redisSpace, watchdogTimeout, watchdogTopic, queue,
        setInterval, clearInterval
    }) {
        super();

        if (watchdogTopic == null || watchdogTopic == '') throw new TypeError('watchdogTopic invalid');
        if (queue == null || queue == '') throw new TypeError('queue invalid');

        this.queue = queue;
        this.sponge = `${this.queue}@${UUID.v1()}`;
        this.watchdogTopic = watchdogTopic;
        this.watchdogTimeout = watchdogTimeout || 30000;
        this.setInterval = setInterval || global.setInterval;
        this.clearInterval = clearInterval || global.clearInterval;

        this.redis = redis || new Redis({host: redisHost , port: redisPort, db: redisSpace});
        assert(this.redis instanceof Redis);
        this.redisConnected = true;

        this.watchdogRedis = watchdogRedis || new Redis({host: watchdogRedisHost, port: watchdogRedisPort, db: watchdogRedisSpace});
        assert(this.watchdogRedis instanceof Redis);

    }

    _hearbeat() {
        return this.watchdogRedis.publish(this.watchdogTopic, JSON.stringify({queue: this.queue, sponge: this.sponge}))
            .catch((e)=> this.emit(e))
    }

    /**
     * Start queue consumer
     * @param consumer function handle queue element and should return a Promise
     * @returns {Promise}
     */
    start(consumer) {
        if (this.redis == null) throw new Error('This message queue has been stopped')
        co(function*() {
            try {
                while (true) {
                    yield this._hearbeat();
                    let element = yield this.redis.brpoplpush(this.queue, this.sponge, parseInt(this.watchdogTimeout/1000) || 1);
                    if (element != null) {
                        yield consumer(element);
                    }
                    yield this.redis.rpop(this.sponge);
                }
            } catch (e) {
                if (this.redis != null) throw e;
            }
        }.bind(this)).catch((e)=>{
            this.emit('error', e);
        });
    }

    /**
     * Stop queue consumer
     */
    stop() {
        if (this.redis) {
            this.clearInterval(this.watchdogInterval);
            this.redis.disconnect();
            this.redis = null;
        }
    }
};
