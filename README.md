# Relief Valve
This package is based on [redis stream](https://github.com/LRagji/redis-streams-broker) data type and provides you with following features
1. It is agnostic to any redis client library can be used with [ioredis](https://www.npmjs.com/package/ioredis) or [redis](https://www.npmjs.com/package/redis) or any of your favourite redis client implementation as long as it satisfies [IRedisClientPool Interface](./tests/utilities/redis-client-pool.ts).
2. This package acts like a pressure relief value used in plumbing; this pattern is used to batch multiple messages in the stream into a single group and deliver it to consumer, used case for data ingestions and other places where batching is needed.
3. It can be used as simple que without batching, for sharding messages within multiple consumers for H-Scalling
4. It provides guarantee for message delivery and or processing with acknowledge facility.
5. Provides facility to reprocess lost messages(which are not acked over a period of time).
6. Can used as in fan-out topology(each consumer receives the copy of the message).
7. Batching of messages with respect to count of messages.
8. Batching of messages with respect to time elapsed from the last write to the stream.
9. Its Redis cluster compatible package.
10. It provides accumalator sharding functionality

## Getting Started

1. Install using `npm -i relief-valve`
2. Require in your project. `const rvType = require('relief-valve').ReliefValve;` or `import { IBatchIdentity, IRedisClient, ReliefValve } from 'relief-valve'`
3. Run redis on local docker if required. `docker run --name streamz -p 6379:6379 -itd --rm redis:latest`
3. Instantiate with a redis client and name for the stream, group name, client name and thresholds. `const publisherInstance = new ReliefValve(client, name, 1, 1, "PubGroup", "Publisher1");`
4. All done, Start using it!!.

## Examples/Code snippets

1. Please find example code usage in [component tests](https://github.com/LRagji/relief-valve/blob/master/tests/component/simple-q-specs.ts)
2. Please find example code implementaion of ioredis client [here](https://github.com/LRagji/relief-valve/blob/master/tests/utilities/redis-client.ts)

```javascript
//Count based batching test
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
```

## Built with

1. Authors :heart: for Open Source.


## Contributions

1. New ideas/techniques are welcomed.
2. Raise a Pull Request.

## Current Version:
0.0.3[Beta]

## License

This project is contrubution to public domain and completely free for use, view [LICENSE.md](/license.md) file for details.

## Quick Tips & Usage Patterns:

1. The package should be instantiated on publisher and subscriber side with identitcal parameters in constructor apart from groupName and clientName, else will lead to chaotic behaviour of pulling messages.
2. Count threshold is always evlauted on writes/publish into stream.
3. Time threshold needs external invocation as redis currently doesnot support cron jobs, either subscriber or publisher can invoke this validation.
4. Highest accurary of time threshold is limited to one second, but depends heavily on external invocation frequency.    

## API
1. Typing info included with the package.
2. Type doc[W.I.P]

## Example impementation of IRedisClientPool

A pooled implmentation

```Typescript
import { IRedisClientPool } from '../../source/index';
import ioredis from 'ioredis';
import fs from 'fs';
import Crypto from "crypto";
export class RedisClientPool implements IRedisClientPool {
    private poolRedisClients: ioredis[];
    private activeRedisClients: Map<string, ioredis>;
    private filenameToCommand = new Map<string, string>();
    private redisConnectionString: string;
    private idlePoolSize: number;
    private totalConnectionCounter = 0;

    constructor(redisConnectionString: string, idlePoolSize = 6) {
        this.poolRedisClients = Array.from({ length: idlePoolSize }, (_) => new ioredis(redisConnectionString));
        this.totalConnectionCounter += idlePoolSize;
        this.activeRedisClients = new Map<string, ioredis>();
        this.redisConnectionString = redisConnectionString;
        this.idlePoolSize = idlePoolSize;
    }

    async shutdown() {
        const waitHandles = [...this.poolRedisClients, ...Array.from(this.activeRedisClients.values())]
            .map(async _ => { await _.quit(); await _.disconnect(); });
        await Promise.allSettled(waitHandles);

        this.poolRedisClients = [];
        this.activeRedisClients.clear();
        this.totalConnectionCounter = 0;
    }

    async acquire(token: string) {
        if (!this.activeRedisClients.has(token)) {
            const availableClient = this.poolRedisClients.pop() || (() => { this.totalConnectionCounter += 1; return new ioredis(this.redisConnectionString); })();
            this.activeRedisClients.set(token, availableClient);
        }
    }

    async release(token: string) {
        if (this.activeRedisClients.has(token)) {
            const releasedClient = this.activeRedisClients.get(token);
            if (releasedClient == undefined) return;
            this.activeRedisClients.delete(token);
            if (this.poolRedisClients.length < this.idlePoolSize) {
                this.poolRedisClients.push(releasedClient);
            }
            else {
                await releasedClient.quit();
                await releasedClient.disconnect();
            }
        }
    }

    async run(token: string, commandArgs: any) {
        const redisClient = this.activeRedisClients.get(token);
        if (redisClient == undefined) {
            throw new Error("Please acquire a client with proper token");
        }
        const v = await redisClient.call(commandArgs.shift(), ...commandArgs);
        return v;
    }

    async pipeline(token: string, commands: string[][]) {
        const redisClient = this.activeRedisClients.get(token);
        if (redisClient == undefined) {
            throw new Error("Please acquire a client with proper token");
        }
        const result = await redisClient.multi(commands)
            .exec();
        const finalResult = result?.map(r => {
            let err = r[0];
            if (err != null) {
                throw err;
            }
            return r[1];
        });
        return finalResult;
    }

    async script(token: string, filename: string, keys: string[], args: any[]) {
        const redisClient = this.activeRedisClients.get(token);
        if (redisClient == undefined) {
            throw new Error("Please acquire a client with proper token");
        }
        let command = this.filenameToCommand.get(filename);
        // @ts-ignore
        if (command == null || redisClient[command] == null) {
            const contents = await new Promise<any>((acc, rej) => {
                fs.readFile(filename, "utf8", (err, data) => {
                    if (err !== null) {
                        rej(err);
                    };
                    acc(data);
                });
            });
            command = Crypto.createHash("sha256").update(contents, "binary").digest("hex")
            redisClient.defineCommand(command, { lua: contents });
            this.filenameToCommand.set(filename, command);
        }
        // @ts-ignore
        const results = await redisClient[command](keys.length, keys, args);
        return results;
    }

    info() {
        const returnObj = {
            "Idle Size": this.idlePoolSize,
            "Current Active": this.activeRedisClients.size,
            "Pooled Connection": this.poolRedisClients.length,
            "Peak Connections": this.totalConnectionCounter
        };
        this.totalConnectionCounter = 0;
        return returnObj;
    }
}
```