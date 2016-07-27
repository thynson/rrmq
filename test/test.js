require('co-mocha');
const {RedisQueueWatchdog, RedisQueueConsumer, RedisQueueProducer} = require('..');
const Redis = require('ioredis');
const co = require('co');
const assert = require('assert');
const _ = require('lodash');
const UUID = require('node-uuid');

const TEST_QUEUE = 'test-queue';
const TEST_SPONGE = 'test-sponge';
const TEST_TOPIC = 'test-watch-dog';

describe('RedisQueueWatchdog', function() {
    "use strict";
    this.slow(5000);
    it('should recover elements from sponge to queue', function* (){

        let redis = new Redis();
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

        yield new Promise (done=> setTimeout(done, 100));
        yield x.stop();
        assert(timeout);
        assert.notEqual(yield redis.llen(TEST_QUEUE), 0);
        assert.equal(yield redis.llen(TEST_SPONGE), 0);
        yield redis.quit();

    });

    it('should should call clearTimeout if heartbeat continues', function* () {

        let redis = new Redis();
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
        yield new Promise (done => setTimeout(done, 100));

        yield redis.publish(TEST_TOPIC, JSON.stringify({sponge: TEST_SPONGE, queue: TEST_QUEUE}));
        yield new Promise (done => setTimeout(done, 100));
        assert.equal(cancelled, true);
        yield x.stop();
        // yield redis.quit();
    })
});
describe('Consumer', function() {
    this.slow(5000);
    it('should be able to start and stop', function*() {
        let consumer = new RedisQueueConsumer({ watchdogTopic: TEST_TOPIC, queue: TEST_QUEUE});
        let received = false;
        consumer.start(co.wrap(function*(message) {
            received = true;
        }));
        yield new Promise (done => setTimeout(done, 100));
        consumer.stop();
        yield new Promise (done => setTimeout(done, 100));
        assert.throws(()=> consumer.start (co.wrap(function*(message) { })), (e)=> e instanceof Error);
    });

    it('should heartbeats', function *() {
        var exception = null;
        this.timeout(5000);
        let consumer = new RedisQueueConsumer({
            watchdogTimeout: 100,
            watchdogTopic: TEST_TOPIC,
            queue: TEST_QUEUE,
        });
        consumer.on('error', (e)=> {console.error(e); exception = e});

        var redis = new Redis();
        yield redis.subscribe(TEST_TOPIC);
        consumer.start(function*(){});
        let counter = 0;
        yield new Promise((done)=>redis.on('message', (topic)=> topic == TEST_TOPIC && ++counter == 2 && done()));
        consumer.stop();
        yield redis.unsubscribe(TEST_TOPIC);
        yield redis.quit();
        assert(exception == null);
    });

});

describe('Consumer and Producer', function () {
    this.slow(1000);
    it('should receive message', function *() {
        "use strict";
        let consumer = new RedisQueueConsumer({ watchdogTopic: TEST_TOPIC, queue: TEST_QUEUE});
        let received = false;
        consumer.start (co.wrap(function*(message) {
            received = true;
        }));
        let producer = new RedisQueueProducer({ queue: TEST_QUEUE});
        yield producer.send('test');
        yield new Promise (done => setTimeout(done, 100));
        consumer.stop();
        assert(received == true);

    });

    it('should emit error event when handler throw exception', function *() {
        "use strict";
        let consumer = new RedisQueueConsumer({ watchdogTopic: TEST_TOPIC, queue: TEST_QUEUE});
        let exception = null;
        consumer.on('error', (e)=>{
            exception = e;
        } );
        consumer.start (co.wrap(function*(message) {
            throw new Error('!!!');
        }));
        let producer = new RedisQueueProducer({ queue: TEST_QUEUE});
        yield producer.send('test');
        yield new Promise (done => setTimeout(done, 100));
        consumer.stop();
        assert(exception != null);

    });

    it('should not receive message exactly as passed', function *() {
        "use strict";
        this.timeout(5000);
        this.slow(5000);
        let consumers = _.times(100).map(x=>new RedisQueueConsumer({ watchdogTopic: TEST_TOPIC, queue: TEST_QUEUE}));
        let receivedMessage = [];
        let sentMessage = [];
        consumers.forEach(consumer=> {
            consumer.start(co.wrap(function*(message) {
                receivedMessage.push(message);
            }));
        });
        let producers = _.times(100).map(x=>new RedisQueueProducer({ queue: TEST_QUEUE}));
        for (let i = 0; i < 5000; i++) {
            let message = UUID.v1();
            sentMessage.push(message);
            yield producers[Date.now()%100].send(message);
        }
        yield new Promise (done => setTimeout(done, 500));
        assert.deepEqual(sentMessage.sort(), receivedMessage.sort());
        consumers.forEach(consumer=>consumer.stop());
    });

});
