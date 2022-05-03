import * as assert from 'assert';
import { IBatchIdentity, IRedisClient, ReliefValve } from '../../source/index';
import { RedisClient } from '../utilities/redis-client'
let client: IRedisClient;
const name = "TestStream";
const delay = (timeInMillis: number) => new Promise((acc, rej) => setTimeout(acc, timeInMillis));

describe(`relief-valve component tests`, () => {

    beforeEach(async function () {
        client = new RedisClient(process.env.REDISCON as string);
        await client.run(["FLUSHALL"]);
    });

    afterEach(async function () {
        await client.shutdown();
    });

    it('should be able to publish data in shared que, and consume the same with single consumer', async () => {
        //Setup
        const publisherInstance = new ReliefValve(client, name, 1, 1, "PubGroup", "Publisher1");
        const consumerInstance1 = new ReliefValve(client, name, 1, 1, "ShardGroup1", "Consumer1");
        const consumerInstance2 = new ReliefValve(client, name, 1, 1, "ShardGroup1", "Consumer2");
        const payload = { "hello": "world1", "A": "1", "Z": "26", "B": "2" };
        const generatedId = await publisherInstance.publish(payload);

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
        assert.strictEqual(consumer1Result.readsInCurrentGroup, 1);
        assert.strictEqual(consumer1Result.payload.has(generatedId), true);
        assert.deepStrictEqual(consumer1Result.payload.get(generatedId), payload);
        assert.strictEqual(consumer2Result, undefined); //Since the first consumer in the group got the message no one else will in the same group(shared behaviour) unless it times out.
        assert.deepStrictEqual(consumer1SecondResult, undefined);
    });

    it('should pass on the message to other consumer if the processing takes more than idle time within same group.', async () => {
        //Setup
        const publisherInstance = new ReliefValve(client, name, 1, 1, "PubGroup", "Publisher1");
        const consumerInstance1 = new ReliefValve(client, name, 1, 1, "ShardGroup1", "Consumer1");
        const consumerInstance2 = new ReliefValve(client, name, 1, 1, "ShardGroup1", "Consumer2");
        const consumerInstance3 = new ReliefValve(client, name, 1, 1, "ShardGroup2", "Consumer1");
        const payload = { "hello": "world1", "A": "1", "Z": "26", "B": "2" };
        const generatedId = await publisherInstance.publish(payload);

        //Test
        const consumer1Result = await consumerInstance1.consumeFreshOrStale(3600);
        const consumer2Result = await consumerInstance2.consumeFreshOrStale(3600);
        await delay(1500);
        const consumer3TimeoutResult = await consumerInstance3.consumeFreshOrStale(1);
        await delay(1500);
        const consumer2TimeoutResult = await consumerInstance2.consumeFreshOrStale(1);

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
        assert.strictEqual(consumer1Result.readsInCurrentGroup, 1);
        assert.strictEqual(consumer1Result.payload.has(generatedId), true);
        assert.deepStrictEqual(consumer1Result.payload.get(generatedId), payload);
        assert.strictEqual(consumer2Result, undefined); //Since the first consumer in the group got the message no one else will in the same group(shared behaviour) unless it times out.
        if (consumer2TimeoutResult == undefined) throw new Error("Read failed no batch found for timeout");
        assert.deepStrictEqual(consumer2TimeoutResult.id, consumer1Result.id);
        assert.deepStrictEqual(consumer2TimeoutResult.name, consumer1Result.name);
        assert.deepStrictEqual(consumer2TimeoutResult.readsInCurrentGroup, 2);
        assert.deepStrictEqual(consumer2TimeoutResult.payload, consumer1Result.payload);
        if (consumer3TimeoutResult == undefined) throw new Error("Read failed no batch found for timeout");
        assert.deepStrictEqual(consumer3TimeoutResult.id, consumer1Result.id);
        assert.deepStrictEqual(consumer3TimeoutResult.name, consumer1Result.name);
        assert.deepStrictEqual(consumer3TimeoutResult.readsInCurrentGroup, 1); //Since this is a fresh read for another group so fresh delivery
        assert.deepStrictEqual(consumer3TimeoutResult.payload, consumer1Result.payload);
    }).timeout(4000);

    it('should not deliver the same message when its acked in the same group.', async () => {
        //Setup
        const publisherInstance = new ReliefValve(client, name, 1, 1, "PubGroup", "Publisher1");
        const consumerInstance1 = new ReliefValve(client, name, 1, 1, "ShardGroup1", "Consumer1");
        const consumerInstance2 = new ReliefValve(client, name, 1, 1, "ShardGroup1", "Consumer2");
        const payload = { "hello": "world1", "A": "1", "Z": "26", "B": "2" };
        const generatedId = await publisherInstance.publish(payload);

        //Test
        const consumer1Result = await consumerInstance1.consumeFreshOrStale(3600);
        const ackResult = await consumerInstance1.acknowledge(consumer1Result as IBatchIdentity);
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
        assert.strictEqual(consumer1Result.readsInCurrentGroup, 1);
        assert.strictEqual(consumer1Result.payload.has(generatedId), true);
        assert.deepStrictEqual(consumer1Result.payload.get(generatedId), payload);
        assert.deepStrictEqual(ackResult, true);
        assert.strictEqual(consumer2Result, undefined); //Since the first consumer in the group got the message no one else will in the same group(shared behaviour) unless it times out.
        assert.strictEqual(consumer1SecondResult, undefined);
    });

});