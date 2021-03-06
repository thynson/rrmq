
import Redis = require('ioredis');
import assert = require('assert');
import * as tuid from 'tuid';
import {RedisQueueWatchdog, RedisQueueConsumer, RedisQueueProducer} from '..';



const TEST_QUEUE = 'test-queue';
const TEST_SPONGE = 'test-sponge';
const TEST_TOPIC = 'test-watch-dog';
const consumerOpt = { watchdogTopic: TEST_TOPIC, queue: TEST_QUEUE, watchdogTimeout: 1000};


describe('RedisQueueWatchdog', function(this: Mocha.ISuiteCallbackContext) {
    "use strict";
    this.slow(3000).timeout(5000);

    it('should recover elements from sponge to queue', async function() {

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

    it('should should call clearTimeout if heartbeat continues', async function() {

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

    it('should start/stop normally in any state', async function () {
        let watchdog = new RedisQueueWatchdog(consumerOpt);
        watchdog.start();
        await watchdog.start();
        await watchdog.start();
        watchdog.start();
        watchdog.stop();
        watchdog.stop();
        await watchdog.stop();
        await watchdog.stop();
        watchdog.stop();
    });
});
describe('Consumer', function(this:Mocha.ISuiteCallbackContext) {
    this.slow(3000).timeout(5000);

    it('should be able to start and stop', async function(this:Mocha.ISuiteCallbackContext){
        let consumer = new RedisQueueConsumer(consumerOpt);
        await consumer.start(async() => null);
        await new Promise (done => setTimeout(done, 100));
        await consumer.stop();
    });

    it('should heartbeats', async function(this:Mocha.ITestCallbackContext) {
        let exception = null;
        let consumer = new RedisQueueConsumer(consumerOpt);
        consumer.on('error', (e)=> {console.error(e); exception = e});
        let redis = new Redis();
        await redis.subscribe(TEST_TOPIC);
        await consumer.start(async () => undefined);
        let counter = 0;
        await new Promise((done)=>redis.on('message', (topic)=> topic == TEST_TOPIC && ++counter == 2 && done()));
        await consumer.stop();
        await redis.unsubscribe(TEST_TOPIC);
        await redis.quit();
        assert(exception == null);
    });

    it('should start/stop normally in any state', async function(this:Mocha.ITestCallbackContext) {
        let consumer = new RedisQueueConsumer(consumerOpt);
        consumer.start(async () => undefined);
        await consumer.start(async () => undefined);
        await consumer.start(async () => undefined);
        consumer.start(async () => undefined);
        consumer.stop();
        consumer.stop();
        await consumer.stop();
        await consumer.stop();
        consumer.stop();
    });
});

describe('Consumer and Producer', function (this:Mocha.ISuiteCallbackContext) {
    this.slow(5000).timeout(10000);
    it('should receive message', async function(){
        "use strict";
        let consumer = new RedisQueueConsumer(consumerOpt);
        let received = false;
        await consumer.start (async ()=> received = true);
        let producer = new RedisQueueProducer({ queue: TEST_QUEUE});
        await producer.send('test');
        await new Promise (done => setTimeout(done, 100));
        await consumer.stop();
        assert(received);

    });

    it('should emit error event when handler throw exception', async function() {
        let consumer = new RedisQueueConsumer(consumerOpt);
        let exception = null;
        consumer.on('error', (e)=>{
            exception = e;
        } );
        await consumer.start (async message => { throw new Error(`Receive message: ${message}`);});
        let producer = new RedisQueueProducer({ queue: TEST_QUEUE});
        await producer.send('test');
        await new Promise (done => setTimeout(done, 100));
        await consumer.stop();
        assert(exception != null);
    });

    it('should continue receive message after error occurred', async function() {
        let watchdog = new RedisQueueWatchdog(consumerOpt);
        await watchdog.start();
        let consumer = new RedisQueueConsumer(consumerOpt);
        consumer.once('error', (e)=> { });
        let state = 0;
        await Promise.all([ new Promise((done)=> {consumer.start (async message => {
            switch(state) {
                case 0:
                    state++;
                    assert(message == 'failed');
                    throw new Error(`Receive message: ${message}`);
                case 1:
                    state++;
                    assert(message == 'success');
                    return ;
                case 2:
                    assert(message == 'failed');
                    done();
                    return ;
            }
        }); }), (async ()=> {
            let producer = new RedisQueueProducer({queue: TEST_QUEUE});
            await producer.send('failed');
            await producer.send('success');
        })()]);

        await consumer.stop();
        await watchdog.stop();
    });

    it('should not receive message exactly as passed', async function() {
        "use strict";
        let values = [];
        for (let i = 0; i < 100; i++) values.push(i);
        let consumers = values.map(x=>new RedisQueueConsumer(consumerOpt));
        let receivedMessage = [];
        let sentMessage = [];
        const MAX_MESSAGE_COUNT = 5000;
        let receivedCount = 0;
        await new Promise((done)=> {

            consumers.forEach(consumer=> {
                return consumer.start(async message =>{
                    receivedMessage.push(message);
                    receivedCount++;
                    if (receivedCount == MAX_MESSAGE_COUNT) done();
                });
            });
            let producers = values.map(x=>new RedisQueueProducer({ queue: TEST_QUEUE}));
            for (let i = 0; i < MAX_MESSAGE_COUNT; i++) {
                let message = new tuid.Generator().generateSync().toString();
                sentMessage.push(message);
                producers[Date.now()%100].send(message);
            }
        });
        assert.deepEqual(sentMessage.sort(), receivedMessage.sort());
        await Promise.all(consumers.map(c=>c.stop));
    });

});
