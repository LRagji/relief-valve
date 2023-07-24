import * as assert from 'assert';
import { IRedisClientPool, IORedisClientPool } from 'redis-abstraction';
import { IBatchIdentity, ReliefValve } from '../../source/index';
let client: IRedisClientPool;
const name = "TestStream";
const delay = (timeInMillis: number) => new Promise((acc, rej) => setTimeout(acc, timeInMillis));

describe(`relief-valve component tests`, () => {

    // Testing includes:
    // Count Batch Mode.
    // Time Batch Mode

    beforeEach(async function () {
        client = new IORedisClientPool(() => IORedisClientPool.IORedisClientClusterFactory([process.env.REDISCON as string]), 2);
        const token = "ST" + Date.now();
        await client.acquire(token);
        try {
            await client.run(token, ["FLUSHALL"]);
        }
        finally {
            await client.release(token);
        }
    });

    afterEach(async function () {
        await client.shutdown();
    });

    it('should be able to publish data in que, and consume the same with specified batch size', async () => {
        //Setup
        const batchsize = 10;
        const publisherInstance = new ReliefValve(client, name, batchsize, 1, "PubGroup", "Publisher1");
        const consumerInstance1 = new ReliefValve(client, name, batchsize, 1, "ShardGroup1", "Consumer1");

        let payloads = new Map<string, object>();
        for (let counter = 0; counter < 100; counter++) {

            const payload = { "hello": "world1", "A": "1", "Z": "26", "B": "2", "counter": counter.toString() };
            const generatedId = await publisherInstance.publish(payload);
            payloads.set(generatedId, payload);

            //Test
            const consumer1Result = await consumerInstance1.consumeFreshOrStale(3600);

            //Verify
            assert.notStrictEqual(generatedId, undefined);
            assert.notStrictEqual(generatedId, null);
            assert.notStrictEqual(generatedId, "");
            if (payloads.size === batchsize) {
                if (consumer1Result == undefined) throw new Error("Read failed no batch found");
                assert.notStrictEqual(consumer1Result.id, undefined);
                assert.notStrictEqual(consumer1Result.id, null);
                assert.notStrictEqual(consumer1Result.id, "");
                assert.strictEqual(consumer1Result.readsInCurrentGroup, 1);
                assert.strictEqual(consumer1Result.payload.size, batchsize);
                assert.deepStrictEqual(consumer1Result.payload, payloads);
                const ackResult = await consumerInstance1.acknowledge(consumer1Result as IBatchIdentity);
                assert.deepStrictEqual(ackResult, true);
                payloads = new Map<string, object>();
            }
            else {
                assert.deepStrictEqual(consumer1Result, undefined);
            }
        }
        const token = "T2" + Date.now();
        await client.acquire(token);
        try {
            const keys = await client.run(token, ["KEYS", "*"]) as Array<string>;
            const mainQlength = await client.run(token, ["XLEN", name]);
            const accQlength = await client.run(token, ["XLEN", (name + "Acc")]);
            assert.deepStrictEqual(keys.length, 3);
            assert.deepStrictEqual(keys.indexOf(name) >= 0, true);
            assert.deepStrictEqual(keys.indexOf((name + "Acc")) >= 0, true);
            assert.deepStrictEqual(keys.indexOf((name + "Idx")) >= 0, true);
            assert.deepStrictEqual(mainQlength, 0);
            assert.deepStrictEqual(accQlength, 0);
        }
        finally {
            await client.release(token);
        }

    });

    it('should be able to publish data in que, and consume the same when specified time elapsed is reached post last write', async () => {
        //Setup
        const batchsize = 10;
        const timeElapsed = 10;
        const tolerance = 1;
        const minTolerance = (timeElapsed - tolerance) * 1000;
        const maxTolerance = (timeElapsed + tolerance) * 1000;
        const publisherInstance = new ReliefValve(client, name, batchsize, timeElapsed, "PubGroup", "Publisher1");
        const consumerInstance1 = new ReliefValve(client, name, batchsize, timeElapsed, "ShardGroup1", "Consumer1");

        const publishTime = Date.now();;
        const payload = { "hello": "world1", "A": "1", "Z": "26", "B": "2", "time": publishTime.toString() };
        const generatedId = await publisherInstance.publish(payload);
        //Verify
        assert.notStrictEqual(generatedId, undefined);
        assert.notStrictEqual(generatedId, null);
        assert.notStrictEqual(generatedId, "");

        let payloads = new Map<string, object>();
        payloads.set(generatedId, payload);
        for (let seconds = 0; seconds < 100; seconds++) {

            //Test
            await delay(1000);
            await consumerInstance1.recheckTimeThreshold();
            const consumer1Result = await consumerInstance1.consumeFreshOrStale(3600);


            if (consumer1Result != undefined) {
                const acquiredTime = Date.now();
                assert.notStrictEqual(consumer1Result.id, undefined);
                assert.notStrictEqual(consumer1Result.id, null);
                assert.notStrictEqual(consumer1Result.id, "");
                assert.strictEqual(consumer1Result.readsInCurrentGroup, 1);
                assert.strictEqual(consumer1Result.payload.size, 1);
                assert.deepStrictEqual(consumer1Result.payload, payloads);
                const waitTime = acquiredTime - publishTime;
                assert.deepStrictEqual((minTolerance <= waitTime && waitTime <= maxTolerance), true, `Elapsed time took: ${waitTime} expected between: ${minTolerance} and ${maxTolerance}`);
                const ackResult = await consumerInstance1.acknowledge(consumer1Result as IBatchIdentity);
                assert.deepStrictEqual(ackResult, true);
                payloads = new Map<string, object>();
                break;
            }
            else {
                assert.deepStrictEqual(consumer1Result, undefined);
            }
        }
        const token = "T3" + Date.now();
        await client.acquire(token);
        try {
            const keys = await client.run(token, ["KEYS", "*"]) as Array<string>;
            const mainQlength = await client.run(token, ["XLEN", name]);
            const accQlength = await client.run(token, ["XLEN", (name + "Acc")]);
            assert.deepStrictEqual(keys.length, 3);
            assert.deepStrictEqual(keys.indexOf(name) >= 0, true);
            assert.deepStrictEqual(keys.indexOf((name + "Acc")) >= 0, true);
            assert.deepStrictEqual(keys.indexOf((name + "Idx")) >= 0, true);
            assert.deepStrictEqual(mainQlength, 0);
            assert.deepStrictEqual(accQlength, 0);
        }
        finally {
            await client.release(token);
        }

    }).timeout(15000);

    it('should be able to release only specified number of items when count threshold is breached.', async () => {
        //Setup
        const testTarget = new ReliefValve(client, name, 10, 2, "PubGroup", "Publisher1", undefined, undefined, undefined, undefined, 1);
        const publishedIdSequence = new Array<string>();
        const payload = { "hello": "world1", "A": "1", "Z": "26", "B": "2" };
        for (let index = 1; index < 100; index++) {
            const id = `0-${index}`;
            await testTarget.publish(payload, id);
            publishedIdSequence.push(id);
            const notification = await testTarget.consumeFreshOrStale(60);
            if (publishedIdSequence.length >= 10) {
                const expectedId = publishedIdSequence.shift() as string;
                assert.deepStrictEqual(notification?.payload.size, 1);
                const notifiedPayload = notification?.payload.get(expectedId);
                assert.deepStrictEqual(notifiedPayload, payload);
                await testTarget.acknowledge(notification);
            }
            else {
                assert.deepStrictEqual(notification, undefined);
            }
        }
        assert.deepStrictEqual(publishedIdSequence.length, 9);
        await delay(3000);
        //Time Purge validate
        const notification = await testTarget.consumeFreshOrStale(60);
        assert.deepStrictEqual(notification, undefined);
        while (publishedIdSequence.length > 0) {
            await testTarget.recheckTimeThreshold();
            const notification = await testTarget.consumeFreshOrStale(60);
            assert.notDeepStrictEqual(notification, undefined);
            const expectedId = publishedIdSequence.shift() as string;
            assert.deepStrictEqual(notification?.payload.size, 1);
            const notifiedPayload = notification?.payload.get(expectedId);
            assert.deepStrictEqual(notifiedPayload, payload);
            await testTarget.acknowledge(notification);
        }

        assert.deepStrictEqual(publishedIdSequence.length, 0);
    }).timeout(5000);

    it('should be able to release only specified number of items when count threshold is breached similar for time threshold.', async () => {
        //Setup
        const testTarget = new ReliefValve(client, name, 10, 2, "PubGroup", "Publisher1", undefined, undefined, undefined, undefined, 1);
        const publishedIdSequence = new Array<string>();
        const payload = { "hello": "world1", "A": "1", "Z": "26", "B": "2" };
        for (let index = 1; index < 100; index++) {
            const id = `0-${index}`;
            await testTarget.publish(payload, id);
            publishedIdSequence.push(id);
            const notification = await testTarget.consumeFreshOrStale(60);
            if (publishedIdSequence.length >= 10) {
                const expectedId = publishedIdSequence.shift() as string;
                assert.deepStrictEqual(notification?.payload.size, 1);
                const notifiedPayload = notification?.payload.get(expectedId);
                assert.deepStrictEqual(notifiedPayload, payload);
                await testTarget.acknowledge(notification);
            }
            else {
                assert.deepStrictEqual(notification, undefined);
            }
        }
        assert.deepStrictEqual(publishedIdSequence.length, 9);
        await delay(3000);
        //Time Purge validate
        const notification = await testTarget.consumeFreshOrStale(60);
        assert.deepStrictEqual(notification, undefined);
        while (publishedIdSequence.length > 0) {
            await testTarget.recheckTimeThreshold(0);
            const notification = await testTarget.consumeFreshOrStale(60);
            if (notification === undefined) {
                await delay(3000);
            }
            else {
                const expectedId = publishedIdSequence.shift() as string;
                assert.deepStrictEqual(notification?.payload.size, 1);
                const notifiedPayload = notification?.payload.get(expectedId);
                assert.deepStrictEqual(notifiedPayload, payload);
                await testTarget.acknowledge(notification);
            }
        }

        assert.deepStrictEqual(publishedIdSequence.length, 0);
    }).timeout(5000 + (3000 * 9));
});