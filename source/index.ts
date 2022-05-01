
export interface IRedisClient {
    acquire(token?: string): Promise<void>
    release(token?: string): Promise<void>
    shutdown(): Promise<void>
    run(commandArgs: string[]): Promise<any>
    pipeline(commands: string[][]): Promise<any>;
    script(filename: string, keys: string[], args: string[]): Promise<any>
}

export interface IBatch {
    name: string,
    id: string,
    payload: string[]
}

export class ReliefValve {

    private writeScript = "write_count_purge.lua";
    private groupsCreated = new Set();

    constructor(private client: IRedisClient,
        private name: string,
        private countThreshold: number,
        private timeThresholdInSeconds: number,
        private indexKey = name + "Idx",
        private accumalatorKey = (data: object) => Promise.resolve(name + "Acc"),
        private accumalatorPurgedKey = (accumalatorKey: string) => Promise.resolve(accumalatorKey + Date.now().toString())) {

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
            const accumulatorKeys = await this.client.run(["ZRANGE", this.indexKey, "-inf", (redisTime - this.timeThresholdInSeconds).toString(), "BYSCORE"]);
            await Promise.allSettled(accumulatorKeys.map(async (accKey: string) => {
                const accKeyRenamed = await this.accumalatorPurgedKey(accKey);
                const keys = [this.indexKey, accKey, accKeyRenamed, this.name];
                if (keys.length != Array.from((new Set(keys)).values()).length) {
                    throw new Error("Name, IndexKey, AccumalatorKey and AccumalatorPurgedKey cannot be same.")
                }
                await this.client.pipeline([
                    ["RENAME", accKey, accKeyRenamed],
                    ["ZREM", this.indexKey, accKey],
                    ["XADD", this.name, "*", "Key", accKeyRenamed]
                ]);
                return 0;
            }));
        }
        finally {
            await this.client.release(token);
        }
    }

    public async consumeFreshOrPending(): Promise<IBatch> {
        throw new Error("TBI");
    }

    public async acknowledge(groupId: string, batch: IBatch): Promise<boolean> {
        const token = `acknowledge-${Date.now().toString()}`;
        await this.client.acquire(token);
        try {
            await this.createStreamGroupIfNotExists(this.name, groupId, this.client);
            const response = await this.client.run(["XACK", this.name, groupId, batch.id]);
            if (response === 1) {
                await this.client.run(["DEL", batch.name]);
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