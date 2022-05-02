import * as assert from 'assert';
import { IRedisClient, ReliefValve } from '../../dist/index';
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

    it('should be able to batch data according to the count threshold and then relief it', async () => {
        //Setup
        const target = new ReliefValve(client, name, countThreshold, timeThreshold, groupName, clientName);
        // const data = new Array<IDimentionalData>();
        // data.push({ dimensions: [1n, 0n, 0n], payload: "A", bytes: 1n });
        // data.push({ dimensions: [11n, 0n, 0n], payload: "B", bytes: 1n });
        // data.push({ dimensions: [21n, 0n, 0n], payload: "C", bytes: 1n });

        // //Test
        // const setResult = await target.write(data);
        // const rangeResult = await target.rangeRead([0n, 0n, 0n], [100n, 100n, 100n]);

        // //Verify
        // assert.deepStrictEqual(setResult.failed.length, 0);
        // assert.deepStrictEqual(setResult.succeeded, data);
        // assert.deepStrictEqual(rangeResult.error, undefined);
        // const readData = data.map(e => { delete e.bytes; return e });
        // assert.deepStrictEqual(rangeResult.data, readData);
    });

});