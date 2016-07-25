require('co-mocha');
const RedisQueueWatchdog = require('..');
const Redis = require('ioredis');
const co = require('co');
const assert = require('assert');

const redis = new Redis();
const TEST_QUEUE = 'test-queue';
const TEST_SPONGE = 'test-sponge';
const TEST_TOPIC = 'test-watch-dog';

describe('RedisQueueWatchdog', function() {
    "use strict";
    this.slow(2000);
    it('should recover elements from sponge to queue', function* (){
        let timeout = false;
        let x = new RedisQueueWatchdog({
            watchdogTopic: TEST_TOPIC,
            setTimeout: (f)=> setImmediate(()=> { timeout = true; f()}),
            clearTimeout: (f)=> clearImmediate(f)
        });
        yield x.start();

        yield redis.del(TEST_QUEUE);
        yield redis.del(TEST_SPONGE);
        yield redis.lpush(TEST_QUEUE, '1');
        yield redis.rpoplpush(TEST_QUEUE, TEST_SPONGE);
        assert.equal(yield redis.llen(TEST_QUEUE),0);
        assert.equal(yield redis.llen(TEST_SPONGE), 1);
        yield redis.publish(TEST_TOPIC, JSON.stringify({sponge: TEST_SPONGE, queue: TEST_QUEUE}));
        yield new Promise (done=> setTimeout(done, 500));
        yield x.stop();
        assert(timeout);
        assert.notEqual(yield redis.llen(TEST_QUEUE), 0);
        assert.equal(yield redis.llen(TEST_SPONGE), 0);
    });

    it('should should call clearTimeout if heartbeat continues', function* () {
        let cancelled = false;

        let x = new RedisQueueWatchdog({
            watchdogTopic: TEST_TOPIC,
            setTimeout: setTimeout,
            clearTimeout: (f)=> { cancelled = true; clearTimeout(f); }
        });
        yield x.start();


        yield redis.del(TEST_QUEUE);
        yield redis.del(TEST_SPONGE);
        yield redis.lpush(TEST_QUEUE, '1');
        yield redis.rpoplpush(TEST_QUEUE, TEST_SPONGE);
        yield redis.publish(TEST_TOPIC, JSON.stringify({sponge: TEST_SPONGE, queue: TEST_QUEUE}));

        assert.equal(cancelled, false);
        yield new Promise (done => setTimeout(done, 500));

        yield redis.publish(TEST_TOPIC, JSON.stringify({sponge: TEST_SPONGE, queue: TEST_QUEUE}));

        yield new Promise (done => setTimeout(done, 500));
        assert.equal(cancelled, true);
        yield x.stop()

    })
});
