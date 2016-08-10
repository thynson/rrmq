/// <reference path="../typings/index.d.ts"/>

import {RedisQueueWatchdog, RedisQueueConsumer, RedisQueueProducer} from '../index';
import * as Redis from 'ioredis';
import * as assert from 'assert';
import * as UUID from 'node-uuid';
import * as sourceMapSupport from 'source-map-support'
sourceMapSupport.install();

const TEST_QUEUE = 'test-queue';
const TEST_SPONGE = 'test-sponge';
const TEST_TOPIC = 'test-watch-dog';

describe('RedisQueueWatchdog', function(this: Mocha) {
    "use strict";
    this.slow(5000);
    it('should recover elements from sponge to queue', async () =>{

        let redis = new Redis();
        let timeout = false;
        let x = new RedisQueueWatchdog({
            watchdogTopic: TEST_TOPIC,
            setTimeout: (f)=> setImmediate(()=> { timeout = true; f()}),
            clearTimeout: (f)=> clearImmediate(f)
        });
        await x.start();

        await redis.del(TEST_QUEUE);
        await redis.del(TEST_SPONGE);
        await redis.lpush(TEST_QUEUE, '1');
        await redis.rpoplpush(TEST_QUEUE, TEST_SPONGE);
        assert.equal(await redis.llen(TEST_QUEUE),0);
        assert.equal(await redis.llen(TEST_SPONGE), 1);
        await redis.publish(TEST_TOPIC, JSON.stringify({sponge: TEST_SPONGE, queue: TEST_QUEUE}));
        await new Promise (done=> setTimeout(done, 100));
        await x.stop();
        assert(timeout);
        assert.notEqual(await redis.llen(TEST_QUEUE), 0);
        assert.equal(await redis.llen(TEST_SPONGE), 0);
        await redis.quit();

    });

    it('should should call clearTimeout if heartbeat continues', async ()=> {

        let redis = new Redis();
        let cancelled = false;

        let x = new RedisQueueWatchdog({
            watchdogTopic: TEST_TOPIC,
            setTimeout: setTimeout,
            clearTimeout: (f)=> { cancelled = true; clearTimeout(f); }
        });
        await x.start();


        await redis.del(TEST_QUEUE);
        await redis.del(TEST_SPONGE);
        await redis.lpush(TEST_QUEUE, '1');
        await redis.rpoplpush(TEST_QUEUE, TEST_SPONGE);
        await redis.publish(TEST_TOPIC, JSON.stringify({sponge: TEST_SPONGE, queue: TEST_QUEUE}));

        assert.equal(cancelled, false);
        await new Promise (done => setTimeout(done, 100));

        await redis.publish(TEST_TOPIC, JSON.stringify({sponge: TEST_SPONGE, queue: TEST_QUEUE}));
        await new Promise (done => setTimeout(done, 100));
        assert.equal(cancelled, true);
        await x.stop();
        await redis.quit();
    });
});
describe('Consumer', function(this:Mocha) {
    this.slow(5000);

    it('should be able to start and stop', async () =>{
        let consumer = new RedisQueueConsumer({ watchdogTopic: TEST_TOPIC, queue: TEST_QUEUE});
        let received = false;
        consumer.start(async() =>{
            received = true;
        });
        await new Promise (done => setTimeout(done, 100));
        consumer.stop();
        await new Promise (done => setTimeout(done, 100));
        try {
            await consumer.start(async ()=> null);
        } catch (e) {
            assert(e instanceof Error);
            return;
        }
        assert(false, "unreachable");
    });

    it('should heartbeats', async () => {
        var exception = null;
        this.timeout(5000);
        let consumer = new RedisQueueConsumer({
            watchdogTimeout: 100,
            watchdogTopic: TEST_TOPIC,
            queue: TEST_QUEUE,
        });
        consumer.on('error', (e)=> {console.error(e); exception = e});

        var redis = new Redis();
        await redis.subscribe(TEST_TOPIC);
        consumer.start(async () => undefined);
        let counter = 0;
        await new Promise((done)=>redis.on('message', (topic)=> topic == TEST_TOPIC && ++counter == 2 && done()));
        consumer.stop();
        await redis.unsubscribe(TEST_TOPIC);
        await redis.quit();
        assert(exception == null);
    });

});

describe('Consumer and Producer', function (this:Mocha) {
    this.slow(1000);
    it('should receive message', async () =>{
        "use strict";
        let consumer = new RedisQueueConsumer({ watchdogTopic: TEST_TOPIC, queue: TEST_QUEUE});
        let received = false;
        consumer.start (async ()=> received = true);
        let producer = new RedisQueueProducer({ queue: TEST_QUEUE});
        await producer.send('test');
        await new Promise (done => setTimeout(done, 100));
        consumer.stop();
        assert(received == true);

    });

    it('should emit error event when handler throw exception', async ()=> {
        let consumer = new RedisQueueConsumer({ watchdogTopic: TEST_TOPIC, queue: TEST_QUEUE});
        let exception = null;
        consumer.on('error', (e)=>{
            exception = e;
        } );
        consumer.start (async message => { throw new Error(`Receive message: ${message}`);});
        let producer = new RedisQueueProducer({ queue: TEST_QUEUE});
        await producer.send('test');
        await new Promise (done => setTimeout(done, 100));
        consumer.stop();
        assert(exception != null);

    });

    it('should not receive message exactly as passed', async ()=> {
        "use strict";
        let values = [];
        for (let i = 0; i < 100; i++) values.push(i);
        let consumers = values.map(x=>new RedisQueueConsumer({ watchdogTopic: TEST_TOPIC, queue: TEST_QUEUE}));
        let receivedMessage = [];
        let sentMessage = [];
        const MAX_MESSAGE_COUNT = 5000;
        let receivedCount = 0;
        await new Promise((done)=> {

            consumers.forEach(consumer=> {
                consumer.start(async message =>{
                    receivedMessage.push(message);
                    receivedCount++;
                    if (receivedCount == MAX_MESSAGE_COUNT) done();
                });
            });
            let producers = values.map(x=>new RedisQueueProducer({ queue: TEST_QUEUE}));
            for (let i = 0; i < MAX_MESSAGE_COUNT; i++) {
                let message = UUID.v1();
                sentMessage.push(message);
                producers[Date.now()%100].send(message);
            }
        });
        assert.deepEqual(sentMessage.sort(), receivedMessage.sort());
        consumers.forEach(consumer=>consumer.stop());
    });

});
