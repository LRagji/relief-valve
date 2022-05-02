
export interface IRedisClient {
    acquire(token?: string): Promise<void>
    release(token?: string): Promise<void>
    shutdown(): Promise<void>
    run(commandArgs: string[]): Promise<any>
    pipeline(commands: string[][]): Promise<any>;
    script(filename: string, keys: string[], args: string[]): Promise<any>
}

export interface IBatch extends IBatchIdentity {
    retrivalCount: number, //Number of times the message have been retrived
    payload: Map<string, object>
}
export interface IBatchIdentity {
    name: string, //Key of the accumulator in redis
    id: string, //Id of the main stream
}

export class ReliefValve {

    private writeScript = "write_count_purge.lua";
    private timePurgeScript = "time_purge.lua";
    private groupsCreated = new Set();

    constructor(private client: IRedisClient,
        private name: string,
        private countThreshold: number,
        private timeThresholdInSeconds: number,
        private groupName: string,
        private clientName: string,
        private indexKey = name + "Idx",
        private accumalatorKey = (data: object) => Promise.resolve(name + "Acc"),
        private accumalatorPurgedKey = (accumalatorKey: string) => Promise.resolve(accumalatorKey + Date.now().toString())) {
        if (this.timeThresholdInSeconds < 0) {
            this.timeThresholdInSeconds *= -1;
        }
        if (this.countThreshold < 0) {
            this.countThreshold *= -1;
        }
        if (this.timeThresholdInSeconds = 0) {
            this.timeThresholdInSeconds = 1;
        }
        if (this.countThreshold = 0) {
            this.countThreshold = 1;
        }
    }

    public async publish(data: object, id = "*"): Promise<string> {
        const token = `publish-${Date.now().toString()}`;
        const accKey = await this.accumalatorKey(data);
        const accKeyRenamed = await this.accumalatorPurgedKey(accKey);
        const keys = [this.indexKey, accKey, accKeyRenamed, this.name];
        if (keys.length != Array.from((new Set(keys)).values()).length) {
            throw new Error("Name, IndexKey, AccumalatorKey and AccumalatorPurgedKey cannot be same.")
        }
        const values = Array.from(Object.entries(data)).flat();
        values.unshift([this.countThreshold, id]);
        await this.client.acquire(token);
        try {
            const response = await this.client.script(this.writeScript, keys, values);
            return response[0];
        }
        finally {
            await this.client.release(token);
        }
    }

    public async recheckTimeThreshold(): Promise<void> {
        const token = `recheckTimeThreshold-${Date.now().toString()}`;
        await this.client.acquire(token);
        try {
            const redisTimeResponse = await this.client.run(["TIME"]);
            const redisTime = parseInt(redisTimeResponse[0]);
            const accumulatorKeysWithScore = await this.client.run(["ZRANGE", this.indexKey, "-inf", (redisTime - this.timeThresholdInSeconds).toString(), "BYSCORE", "WITHSCORES"]);
            const asyncHandles = [];
            for (let counter = 0; counter < accumulatorKeysWithScore.length; counter += 2) {
                const accKey: string = accumulatorKeysWithScore[counter];
                const accKeyScore: string = accumulatorKeysWithScore[counter + 1];
                asyncHandles.push((async () => {
                    const accKeyRenamed = await this.accumalatorPurgedKey(accKey);
                    const keys = [this.indexKey, accKey, accKeyRenamed, this.name];
                    if (keys.length != Array.from((new Set(keys)).values()).length) {
                        throw new Error("Name, IndexKey, AccumalatorKey and AccumalatorPurgedKey cannot be same.")
                    }
                    await this.client.script(this.timePurgeScript, keys, [accKeyScore]);// Script is used to ensure serializable transactions and score is passed to make it thread safe with other instances.
                })());
            }
            await Promise.allSettled(asyncHandles);
        }
        finally {
            await this.client.release(token);
        }
    }

    public async consumeFreshOrStale(batchIdealThresholdInSeconds: number): Promise<IBatch | undefined> {
        const token = `consumeFreshOrPending-${Date.now().toString()}`;
        let returnValue: IBatch | undefined = undefined;
        await this.client.acquire(token);
        try {
            await this.createStreamGroupIfNotExists(this.name, this.groupName, this.client);
            const staleResponse = await this.client.run(["XAUTOCLAIM", this.name, this.groupName, this.clientName, (batchIdealThresholdInSeconds * 1000).toString(), "0-0", "COUNT", "1"]);
            let itemToProcess: string[] | undefined = undefined;
            if (Array.isArray(staleResponse) && staleResponse.length >= 2 && Array.isArray(staleResponse[1]) && staleResponse[1].length > 0) {
                //We have a stale response
                itemToProcess = staleResponse[1];
            }
            else {
                //We need to pluck fresh ones.
                const freshResponse = await this.client.run(["XREADGROUP", "GROUP", this.groupName, this.clientName, "COUNT", "1", "STREAMS", this.name, ">"]);
                if (Array.isArray(freshResponse) && freshResponse.length >= 1 && Array.isArray(freshResponse[0]) && freshResponse[0].length >= 2 && freshResponse[1].length > 0) {
                    itemToProcess = freshResponse[1];
                }
            }

            if (itemToProcess != undefined) {
                returnValue = {
                    "id": itemToProcess[0],
                    "name": itemToProcess[1][1],
                    "retrivalCount": -1,
                    "payload": new Map<string, Object>()
                };
                const retrivalCountResponse = await this.client.run(["XPENDING", this.name, this.groupName, returnValue.id, returnValue.id, "1"]);
                if (Array.isArray(retrivalCountResponse) && retrivalCountResponse.length >= 1) {
                    returnValue.retrivalCount = parseInt(retrivalCountResponse[0][3]);
                }
                const serializedPayload = await this.client.run(["XREAD", "COUNT", "1", "STREAMS", returnValue.name, "0-0"]);
                if (Array.isArray(serializedPayload) && serializedPayload.length >= 1 && Array.isArray(serializedPayload[1])) {
                    const entries = serializedPayload[1];
                    returnValue.payload = entries.reduce((acc: Map<string, Object>, entry: any[]) => {
                        const key = entry[0];
                        const serializedObject = entry[1];
                        const pairs = serializedObject.reduce((acc: string[][], e: string, idx: number) => {
                            if (idx % 2 === 0) {
                                const kvp = acc.pop() as string[];
                                kvp.push(e);
                                acc.push(kvp);
                            } else {
                                acc.push([e]);
                            }
                            return acc;
                        }, []);
                        acc.set(key, Object.fromEntries(pairs));
                        return acc;
                    }, returnValue.payload);
                }
            }
            return returnValue;
        }
        finally {
            await this.client.release(token);
        }
    }

    public async acknowledge(batch: IBatchIdentity, dropBatch: true): Promise<boolean> {
        const token = `acknowledge-${Date.now().toString()}`;
        await this.client.acquire(token);
        try {
            await this.createStreamGroupIfNotExists(this.name, this.groupName, this.client);
            const response = await this.client.run(["XACK", this.name, this.groupName, batch.id]);
            if (response === 1) {
                if (dropBatch === true) {
                    await this.client.run(["DEL", batch.name]);
                    await this.client.run(["XDEL", batch.id]);
                }
                return true;
            }
            else {
                return false;
            }
        }
        finally {
            await this.client.release(token);
        }
    }

    private async createStreamGroupIfNotExists(streamName: string, groupName: string, acquiredClient: IRedisClient, cache = this.groupsCreated): Promise<void> {
        if (cache.has(groupName)) {
            return;
        }
        await acquiredClient.run(["XGROUP", "CREATE", streamName, groupName, "0", "MKSTREAM"]);
        cache.add(groupName);
    }
} 