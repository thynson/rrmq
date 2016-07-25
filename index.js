/**
 * Created by lanxingcan on 16/7/25.
 */

const Redis = require('ioredis');
const co = require('co');
const assert = require('assert');
const event = require('events');


module.exports = class RedisQueueWatchdog extends event.EventEmitter {

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
        this.setTimeout = this.setTimeout || setTimeout;
        this.clearTimeout = this.clearTimeout || clearTimeout;

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