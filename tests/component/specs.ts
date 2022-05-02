import * as assert from 'assert';
import { IRedisClient, ReliefValve } from '../../source/index';
import { RedisClient } from '../utilities/redis-client'
let client: IRedisClient;
const name = "TestMojo";
const groupName = "G1";
const clientName = "C1";
const countThreshold = 10;
const timeThreshold = -1;

describe(`relief-valve component tests`, () => {

    beforeEach(async function () {
        client = new RedisClient(process.env.REDISCON as string);
        await client.run(["FLUSHALL"]);
    });

    afterEach(async function () {
        await client.shutdown();
    });

    it('should be able to batch data according to the count threshold and then release it', async () => {
        //Setup
        const target = new ReliefValve(client, name, 2, timeThreshold, groupName, clientName);
        const data1 = { "hello": "world1" };
        const data2 = { "hello": "world2" };
        const redisGeneratedId1 = await target.publish(data1);
        const redisGeneratedId2 = await target.publish(data2);
        const batch = await target.consumeFreshOrStale(3600);

        // const data = new Array<IDimentionalData>();
        // data.push({ dimensions: [1n, 0n, 0n], payload: "A", bytes: 1n });
        // data.push({ dimensions: [11n, 0n, 0n], payload: "B", bytes: 1n });
        // data.push({ dimensions: [21n, 0n, 0n], payload: "C", bytes: 1n });

        // //Test
        // const setResult = await target.write(data);
        // const rangeResult = await target.rangeRead([0n, 0n, 0n], [100n, 100n, 100n]);

        //Verify
        assert.notStrictEqual(undefined, redisGeneratedId1);
        assert.notStrictEqual(null, redisGeneratedId1);
        assert.notStrictEqual(undefined, redisGeneratedId2);
        assert.notStrictEqual(null, redisGeneratedId2);
        console.log(batch);
        // assert.deepStrictEqual(rangeResult.error, undefined);
        // const readData = data.map(e => { delete e.bytes; return e });
        // assert.deepStrictEqual(rangeResult.data, readData);
    });

});