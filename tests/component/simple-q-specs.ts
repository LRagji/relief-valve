import * as assert from 'assert';
import { IRedisClient, ReliefValve } from '../../source/index';
import { RedisClient } from '../utilities/redis-client'
let client: IRedisClient;
const name = "TestStream";

describe(`relief-valve component tests`, () => {

    beforeEach(async function () {
        client = new RedisClient(process.env.REDISCON as string);
        await client.run(["FLUSHALL"]);
    });

    afterEach(async function () {
        await client.shutdown();
    });

    it('should be able to publish data in shared que with single consumer', async () => {
        //Setup
        const publisherInstance = new ReliefValve(client, name, 1, 1, "PubGroup", "Publisher1");
        const consumerInstance1 = new ReliefValve(client, name, 1, 1, "ShardGroup1", "Consumer1");
        const consumerInstance2 = new ReliefValve(client, name, 1, 1, "ShardGroup1", "Consumer1");
        const data = { "hello": "world1" };
        const generatedId = await publisherInstance.publish(data);

        //Test
        const consumer1Result = await consumerInstance1.consumeFreshOrStale(3600);
        const consumer2Result = await consumerInstance2.consumeFreshOrStale(3600);
        const consumer1SecondResult = await consumerInstance1.consumeFreshOrStale(3600);

        //Verify
        assert.notStrictEqual(generatedId, undefined);
        assert.notStrictEqual(generatedId, null);
        assert.notStrictEqual(generatedId, "");
        if (consumer1Result == undefined) throw new Error("Read failed no batch found");
        assert.notStrictEqual(consumer1Result.id, undefined);
        assert.notStrictEqual(consumer1Result.id, null);
        assert.notStrictEqual(consumer1Result.id, "");
        assert.notStrictEqual(consumer1Result.name, undefined);
        assert.notStrictEqual(consumer1Result.name, null);
        assert.notStrictEqual(consumer1Result.name, "");
        assert.strictEqual(consumer1Result.retrivalCount, 1);
        assert.strictEqual(consumer1Result.payload.has(generatedId), true);
        assert.deepStrictEqual(consumer1Result.payload.get(generatedId), data);
        assert.strictEqual(consumer2Result, undefined); //Since the first consumer in the group got the message.
        assert.deepStrictEqual(consumer1SecondResult, undefined);
    });

});