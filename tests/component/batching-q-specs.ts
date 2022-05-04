import * as assert from 'assert';
import { IBatchIdentity, IRedisClient, ReliefValve } from '../../source/index';
import { RedisClient } from '../utilities/redis-client'
let client: IRedisClient;
const name = "TestStream";
const delay = (timeInMillis: number) => new Promise((acc, rej) => setTimeout(acc, timeInMillis));

describe(`relief-valve component tests`, () => {

    // Testing includes:
    // Count Batch Mode.
    // Time Batch Mode

    beforeEach(async function () {
        client = new RedisClient(process.env.REDISCON as string);
        await client.run(["FLUSHALL"]);
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
        const keys = await client.run(["KEYS", "*"]);
        const length = await client.run(["XLEN", name]);
        assert.deepStrictEqual(keys, [name]);
        assert.deepStrictEqual(length, 0);
    });

    it('should be able to publish data in que, and consume the same with specified time elapsed is reached post last write', async () => {
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
        const keys = await client.run(["KEYS", "*"]);
        const length = await client.run(["XLEN", name]);
        assert.deepStrictEqual(keys, [name]);
        assert.deepStrictEqual(length, 0);
    }).timeout(15000);
});