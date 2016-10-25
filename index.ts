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

import * as Redis from 'ioredis';
import * as assert from 'assert';
import * as event from 'events';
import * as tuid from 'tuid';



enum State {
    STOPPED,
    STOPPING,
    STARTED,
    STARTING
}

enum Result {
    OKAY,
    RESTART,
    STOP
}


/**
 * @callback MessageHandler
 * @param message {String}
 * @returns {Promise}
 */
export interface MessageHandler {
    (message: string): Promise<any>;
}

export class RedisQueueWatchdog extends event.EventEmitter {

    private table: Object;
    private watchdogTopic: string;
    private watchdogTimeout: number;
    private setTimeout: typeof global.setTimeout;
    private clearTimeout: typeof global.clearTimeout;
    private redis: IORedis.Redis;
    private watchdogRedis: IORedis.Redis;
    private topicListener: (topic: string, data: string)=> void;
    private state: State = State.STOPPED;

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
    constructor(opt: RedisQueueWatchdog.Config){
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
        this.topicListener = (topic, data) => {
            if (topic == this.watchdogTopic) {

                try {
                    let result = JSON.parse(data);
                    let sponge = result.sponge;
                    let queue = result.queue;
                    let timeoutHandle = this.table[sponge];
                    if (timeoutHandle) {
                        delete this.table[sponge];
                        this.clearTimeout(timeoutHandle);
                    }
                    timeoutHandle = this.setTimeout( ()=> {
                        let fn = ()=> {
                            this.redis.rpoplpush(sponge, queue).then((result) => {
                                if (result != null) return fn();
                            })
                        };
                        fn();
                    }, this.watchdogTimeout * 2);

                    this.table[sponge]= timeoutHandle;

                } catch(e) {
                    this.emit('error', e);
                }
            }
        }
    }

    /**
     * Start the watchdog
     * @returns {Promise}
     */
    start():Promise<void> {
        return new Promise((done, fail)=> {
            switch(this.state) {
                case State.STOPPED:
                    this.state = State.STARTING;
                    this.once('started', done);
                    this.watchdogRedis.addListener('message', this.topicListener);
                    this.watchdogRedis.subscribe(this.watchdogTopic)
                        .then(()=> {
                            this.state = State.STARTED;
                            this.emit('started');
                        })
                        .catch(fail);
                    return;
                case State.STARTING:
                    this.once('started', done);
                    return;
                case State.STARTED:
                    done();
                    return;
                case State.STOPPING:
                    fail(new Error('watchdog is stopping'));
                    return;
            }
        });

    }

    /**
     * Stop the watchdog
     * @returns {Promise}
     */
    stop() {
        return new Promise((done, fail)=> {
            switch(this.state) {
                case State.STOPPED:
                    return done();
                case State.STARTING:
                    return fail(new Error('watchdog is starting'));
                case State.STARTED:
                    this.once('stopped', done);
                    this.state = State.STOPPING;
                    this.watchdogRedis.unsubscribe(this.watchdogTopic)
                        .then(()=>{
                            this.watchdogRedis.removeListener('message', this.topicListener);
                            for (let sponge in this.table) {
                                clearTimeout(this.table[sponge]);
                                delete this.table[sponge];
                            }
                            this.state = State.STOPPED;
                            this.emit('stopped');
                        })
                        .catch(fail);
                    return;
                case State.STOPPING:
                    this.once('stopped', done);
                    return;
            }
        })
    }


}

export namespace RedisQueueWatchdog {
    export interface Config {
        watchdogTopic: string,
        watchdogTimeout?: number,
        setTimeout?: typeof global.setTimeout,
        clearTimeout?: typeof global.clearTimeout,
        redisHost?: string,
        redisPort?: number,
        redisSpace?: number,
        watchdogRedisHost?: string,
        watchdogRedisPort?: number,
        watchdogRedisSpace?: number,
        redis?: IORedis.Redis,
        watchdogRedis?: IORedis.Redis
    }
}


export class RedisQueueProducer extends event.EventEmitter {

    private redis: IORedis.Redis;
    private queue: string;
    /**
     * Creating a watchdog instance
     *
     * @param option.redis Instance of IORedis that will manipulate the queue, can pass null and then create it internally.
     * @param option.redisHost {String} Host for internal IORedis instance
     * @param option.redisPort {Number} Port for internal IORedis instance
     * @param option.redisSpace {Number} Database index for internal IORedis instance
     * @param option.queue {String} The name of the queue in redis
     */
    constructor(option: RedisQueueProducer.Config) {
        super();
        if (option == null || option.queue == null || option.queue == '') throw new TypeError('invalid option');

        this.queue = option.queue;
        this.redis = option.redis || new Redis({host: option.redisHost , port: option.redisPort, db: option.redisSpace});
        assert(this.redis instanceof Redis);
    }

    /**
     * Send message
     * @param message The message
     * @returns {Promise}
     */
    send(message) : Promise<any> {
        return this.redis.lpush(this.queue, message);
    }
}


export namespace RedisQueueProducer {
    export interface Config {
        redis?: IORedis.Redis,
        redisHost?: string,
        redisPort?: number,
        redisSpace?: number,
        queue: string
    }
}

export class RedisQueueConsumer extends event.EventEmitter {

    private watchdogTopic: string;
    private watchdogTimeout?: number;
    private redis: IORedis.Redis | null;
    private watchdogRedis: IORedis.Redis;
    private queue: string;
    private state: State = State.STOPPED;
    private idGenerator: tuid.Generator = new tuid.Generator();


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
    constructor(option: RedisQueueConsumer.Config) {
        super();

        if (option.watchdogTopic == null || option.watchdogTopic == '')
            throw new TypeError('watchdogTopic invalid');
        if (option.queue == null || option.queue == '')
            throw new TypeError('queue invalid');

        this.queue = option.queue;
        this.watchdogTopic = option.watchdogTopic;
        this.watchdogTimeout = option.watchdogTimeout || 30000;

        this.redis = option.redis || new Redis({
            host: option.redisHost , port: option.redisPort, db: option.redisSpace
        });
        assert(this.redis instanceof Redis);

        this.watchdogRedis = option.watchdogRedis || new Redis({
            host: option.watchdogRedisHost, port: option.watchdogRedisPort, db: option.watchdogRedisSpace
        });
        assert(this.watchdogRedis instanceof Redis);

    }

    private _heartbeat(sponge) {
        return this.watchdogRedis.publish(this.watchdogTopic, JSON.stringify({queue: this.queue, sponge}))
            .catch((e)=> this.emit(e))
    }

    /**
     * Start queue consumer
     * @param consumer {MessageHandler} function handle queue element
     */
    start(consumer: MessageHandler):Promise<void> {

        let listenTimeout = Math.ceil(this.watchdogTimeout / 1000 / 2)|| 1;
        let procedure = (identifier)=> {
            let sponge = `${this.queue}@${identifier}`;
            this.state = State.STARTED;
            this.emit('started');
            this._heartbeat(sponge)
                .then(()=> this.redis.brpoplpush(this.queue, sponge, listenTimeout))
                .then((element)=> {
                    if (element == null) {
                        //Timeout, continue
                        return Promise.resolve(Result.OKAY);
                    }

                    return this._heartbeat(sponge)
                        .then(()=> consumer(element)
                        .then(()=>{
                            return this.redis.lrem(sponge, 1, element)
                                .then(()=> {
                                    return Result.OKAY
                                })
                                .catch((e)=>{
                                    this.emit('error', e);
                                    return Result.STOP;
                                });
                        })
                        .catch((e)=> {
                            this.emit('error', e);
                            return Result.RESTART;
                        }));
                })
                .then((result)=>{
                    if (this.state == State.STOPPING)
                        result = Result.STOP;

                    switch(result) {
                        case Result.OKAY:
                            return procedure(identifier);
                        case Result.RESTART:
                            this.idGenerator.generate().then(id=>procedure(id.toString()));
                            return;
                        case Result.STOP:
                            this.state = State.STOPPED;
                            this.emit('stopped');
                            return;
                    }
                });
        };
        return new Promise((done, fail) => {
            switch(this.state) {
                case State.STOPPED:
                    this.state = State.STARTING;
                    process.nextTick(() => this.idGenerator.generate().then((id)=> procedure(id.toString())));
                //noinspection FallthroughInSwitchStatementJS
                case State.STARTING:
                    this.once('started', done);
                    return;
                case  State.STARTED:
                    return done();
                case State.STOPPING:
                    return fail(new Error('Consumer is going to shutdown'));
            }
        });
    }

    /**
     * Stop queue consumer
     */
    stop(): Promise<void> {
        return new Promise((done, fail) => {
            switch(this.state) {
                case State.STOPPED:
                    return done();
                case State.STARTING:
                    return fail(new Error('Consumer is starting up'));
                case State.STARTED:
                    this.state = State.STOPPING;
                //noinspection FallthroughInSwitchStatementJS
                case State.STOPPING:
                    this.once('stopped', done);
            }
        });
    }
}

export namespace RedisQueueConsumer {
    export interface Config {
        watchdogTopic: string;
        watchdogTimeout?: number;
        queue: string;
        redisHost?: string;
        redisPort?: number;
        redisSpace?: number;
        watchdogRedisHost?: string;
        watchdogRedisPort?: number;
        watchdogRedisSpace?: number;
        redis?: IORedis.Redis;
        watchdogRedis?: IORedis.Redis;
    }


}

