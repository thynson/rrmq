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



/**
 * @callback MessageHandler
 * @param message {String}
 * @returns {Promise}
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
     * @param opt.watchdogRedis Instance of IORedis that will enter subscribe mode, can pass null and then create it internally
     * @param opt.watchdogRedisHost {String} Host for internal watchdog IORedis instance
     * @param opt.watchdogRedisPort {Number} Port for internal watchdog IORedis instance
     * @param opt.watchdogRedisSpace {Number} Database index for internal watchdog IORedis instance
     * @param opt.redis Instance of IORedis that will manipulate the queue, can pass null and then create it internally.
     * @param opt.redisHost {String} Host for internal IORedis instance
     * @param opt.redisPort {Number} Port for internal IORedis instance
     * @param opt.redisSpace {Number} Database index for internal IORedis instance
     * @param opt.watchdogTimeout {Number} Timeout of the watchdog
     * @param opt.watchdogTopic {Number} Topic name which watchdog is subscribe to
     * @param opt.setTimeout {Function} setTimeout function, pass a stub if you want to do test.
     * @param opt.clearTimeout {Function} clearTimeout function, pass a stub if you want to do test.
     */
    constructor(opt){
        super();
        this.table = {};
        if (opt.watchdogTopic == null || opt.watchdogTopic == '') throw new TypeError('watchdogTopic invalid');
        this.watchdogTopic = opt.watchdogTopic;
        this.watchdogTimeout = opt.watchdogTimeout || 30000;
        this.setTimeout = opt.setTimeout || setTimeout;
        this.clearTimeout = opt.clearTimeout || clearTimeout;

        this.redis = opt.redis || new Redis({host: opt.redisHost , port: opt.redisPort, db: opt.redisSpace});
        assert(this.redis instanceof Redis);

        this.watchdogRedis = opt.watchdogRedis || new Redis({host: opt.watchdogRedisHost, port: opt.watchdogRedisPort, db: opt.watchdogRedisSpace});
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
     * @param option.redis Instance of IORedis that will manipulate the queue, can pass null and then create it internally.
     * @param option.redisHost {String} Host for internal IORedis instance
     * @param option.redisPort {Number} Port for internal IORedis instance
     * @param option.redisSpace {Number} Database index for internal IORedis instance
     * @param option.queue {String} The name of the queue in redis
     */
    constructor(option) {
        super();
        if (option.queue == null || option.queue == '') throw new TypeError('queue invalid');

        this.queue = option.queue;
        this.redis = option.redis || new Redis({host: option.redisHost , port: option.redisPort, db: option.redisSpace});
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
     * @param option.watchdogRedis Instance of IORedis that will enter subscribe mode, can pass null and then create it internally
     * @param option.watchdogRedisHost {String} Host for internal watchdog IORedis instance
     * @param option.watchdogRedisPort {Number} Port for internal watchdog IORedis instance
     * @param option.watchdogRedisSpace {Number} Database index for internal watchdog IORedis instance
     * @param option.redis Instance of IORedis that will manipulate the queue, can pass null and then create it internally.
     * @param option.redisHost {String} Host for internal IORedis instance
     * @param option.redisPort {Number} Port for internal IORedis instance
     * @param option.redisSpace {Number} Database index for internal IORedis instance
     * @param option.watchdogTimeout {Number} Timeout of the watchdog
     * @param option.watchdogTopic {Number} Topic name which watchdog is subscribe to
     * @param option.queue {String} The name of the queue in redis
     * @param option.setInterval {Function} setInterval function, pass a stub if you want to do test.
     * @param option.clearInterval {Function} clearInterval function, pass a stub if you want to do test.
     */
    constructor(option) {
        super();

        if (option.watchdogTopic == null || option.watchdogTopic == '')
            throw new TypeError('watchdogTopic invalid');
        if (option.queue == null || option.queue == '')
            throw new TypeError('queue invalid');

        this.queue = option.queue;
        this.sponge = `${this.queue}@${UUID.v1()}`;
        this.watchdogTopic = option.watchdogTopic;
        this.watchdogTimeout = option.watchdogTimeout || 30000;
        this.setInterval = option.setInterval || global.setInterval;
        this.clearInterval = option.clearInterval || global.clearInterval;

        this.redis = option.redis || new Redis({
            host: option.redisHost , port: option.redisPort, db: option.redisSpace
        });
        assert(this.redis instanceof Redis);

        this.watchdogRedis = option.watchdogRedis || new Redis({
            host: option.watchdogRedisHost, port: option.watchdogRedisPort, db: option.watchdogRedisSpace
        });
        assert(this.watchdogRedis instanceof Redis);

    }

    _hearbeat() {
        return this.watchdogRedis.publish(this.watchdogTopic, JSON.stringify({queue: this.queue, sponge: this.sponge}))
            .catch((e)=> this.emit(e))
    }

    /**
     * Start queue consumer
     * @param consumer {MessageHandler} function handle queue element and should return a Promise
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
