import path from "path";
/**
* Interface used to abstract a redis client/connection for this package
*/
export interface IRedisClientPool {
    /**
     * This method acquires a redis client instance from a pool of redis clients with token as an identifier/handle.
     * @param token A unique string used to acquire a redis client instance against. Treat this as redis client handle.
     */
    acquire(token: string): Promise<void>
    /**
     * This method releases the acquired redis client back into the pool.
     * @param token A unique string used when acquiring client via {@link acquire} method
     */
    release(token: string): Promise<void>
    /**
     * Signals a dispose method to the pool stating no more clients will be needed, donot call any methods post calling shutdown. 
     */
    shutdown(): Promise<void>
    /**
     * Executes a single command on acquired connection.
     * @param token token string which was used to acquire.
     * @param commandArgs Array of strings including commands and arguments Eg:["set","key","value"]
     * @returns Promise of any type.
     */
    run(token: string, commandArgs: string[]): Promise<any>
    /**
     * This method is used to execute a set of commands in one go sequentially on redis side.
     * @param token token string which was used to acquire.
     * @param commands Array of array of strings including multiple commands and arguments that needs to be executed in one trip to the server sequentially. Eg:[["set","key","value"],["get","key"]]
     * @returns Promise of all results in the commands(any type).
     */
    pipeline(token: string, commands: string[][]): Promise<any>;
    /**
     * This method is used to execute a lua script on redis connection.
     * @param token token string which was used to acquire.
     * @param filename Full file path of the lua script to be executed Eg: path.join(__dirname, "script.lua")
     * @param keys Array of strings, Keys to be passsed to the script. 
     * @param args Array of strings, Arguments to be passed to the script.
     */
    script(token: string, filename: string, keys: string[], args: string[]): Promise<any>
}

/**
 * Abstraction representing collection of multiple message into a batch
 */
export interface IBatch extends IBatchIdentity {
    /** Number of time the message was delivered to a certain consumer group. */
    readsInCurrentGroup: number,
    /** Represents accumalated messages the Key is the ID given at the time of publishing and the value is object*/
    payload: Map<string, object>
}

/**
 * Provides identity to the batch accumalated
 */
export interface IBatchIdentity {
    /** Stream Id used to acknowledge the message. */
    id: string, //Id of the main stream
}

export class ReliefValve {

    private writeScript = path.join(__dirname, "write_count_purge.lua");
    private timePurgeScript = path.join(__dirname, "time_purge.lua");
    private groupsCreated = new Set();

    constructor(private client: IRedisClientPool,
        private name: string,
        private countThreshold: number,
        private timeThresholdInSeconds: number,
        private groupName: string,
        private clientName: string,
        private indexKey = name + "Idx",
        private accumalatorKey = (data: object) => Promise.resolve(name + "Acc"),
        private cappedStreamLength = -1,
        private systemIdPropName = "_id_") {
        if (this.timeThresholdInSeconds < 0) {
            this.timeThresholdInSeconds *= -1;
        }
        if (this.countThreshold < 0) {
            this.countThreshold *= -1;
        }
        if (this.timeThresholdInSeconds === 0) {
            this.timeThresholdInSeconds = 1;
        }
        if (this.countThreshold === 0) {
            this.countThreshold = 1;
        }
    }

    public async publish(data: object, id = "*"): Promise<string> {
        const token = `publish-${Date.now().toString()}`;
        const accKey = await this.accumalatorKey(data);
        const keys = [this.indexKey, accKey, this.name];
        if (keys.length != Array.from((new Set(keys)).values()).length) {
            throw new Error("Name, IndexKey, AccumalatorKey and AccumalatorPurgedKey cannot be same.")
        }
        const values = Array.from(Object.entries(data)).flat();
        const allStrings = values.reduce((acc, e) => acc && typeof (e) === "string", true);
        if (allStrings === false) {
            throw new Error("Publish only support objects having strings as their values.");
        }
        values.unshift(this.cappedStreamLength);
        values.unshift(this.systemIdPropName);
        values.unshift(id);
        values.unshift(this.countThreshold);
        await this.client.acquire(token);
        try {
            const response = await this.client.script(token, this.writeScript, keys, values);
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
            const redisTimeResponse = await this.client.run(token, ["TIME"]);
            const redisTime = parseInt(redisTimeResponse[0]);
            const accumulatorKeysWithScore = await this.client.run(token, ["ZRANGE", this.indexKey, "-inf", (redisTime - this.timeThresholdInSeconds).toString(), "BYSCORE", "WITHSCORES"]);
            const asyncHandles = [];
            for (let counter = 0; counter < accumulatorKeysWithScore.length; counter += 2) {
                const accKey: string = accumulatorKeysWithScore[counter];
                const accKeyScore: string = accumulatorKeysWithScore[counter + 1];
                asyncHandles.push((async () => {
                    const keys = [this.indexKey, accKey, this.name];
                    if (keys.length != Array.from((new Set(keys)).values()).length) {
                        throw new Error("Name, IndexKey, AccumalatorKey and AccumalatorPurgedKey cannot be same.")
                    }
                    await this.client.script(token, this.timePurgeScript, keys, [accKeyScore, this.systemIdPropName]);// Script is used to ensure serializable transactions and score is passed to make it thread safe with other instances.
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
            await this.createStreamGroupIfNotExists(this.name, this.groupName, this.client, token);
            const staleResponse = await this.client.run(token, ["XAUTOCLAIM", this.name, this.groupName, this.clientName, (batchIdealThresholdInSeconds * 1000).toString(), "0-0", "COUNT", "1"]);
            let itemToProcess: string[][] | undefined = undefined;
            if (Array.isArray(staleResponse) && staleResponse.length >= 2 && Array.isArray(staleResponse[1]) && staleResponse[1].length > 0) {
                //We have a stale response
                itemToProcess = staleResponse[1][0];
            }
            else {
                //We need to pluck fresh ones.
                const freshResponse = await this.client.run(token, ["XREADGROUP", "GROUP", this.groupName, this.clientName, "COUNT", "1", "STREAMS", this.name, ">"]);
                if (Array.isArray(freshResponse) && freshResponse.length >= 1 && Array.isArray(freshResponse[0]) && freshResponse[0].length >= 2 && freshResponse[0][1].length > 0) {
                    itemToProcess = freshResponse[0][1][0];
                }
            }

            if (itemToProcess != undefined) {
                returnValue = {
                    "id": itemToProcess[0] as unknown as string,
                    "readsInCurrentGroup": -1,
                    "payload": new Map<string, Object>()
                };
                const retrivalCountResponse = await this.client.run(token, ["XPENDING", this.name, this.groupName, returnValue.id, returnValue.id, "1"]);
                if (Array.isArray(retrivalCountResponse) && retrivalCountResponse.length >= 1) {
                    returnValue.readsInCurrentGroup = parseInt(retrivalCountResponse[0][3]);
                }

                let currentMessageId = "";
                const serializedPayload = itemToProcess[1];
                for (let propCounter = 0; propCounter < serializedPayload.length; propCounter += 2) {
                    const propName = serializedPayload[propCounter];
                    const propValue = serializedPayload[propCounter + 1];
                    if (propName === this.systemIdPropName) {
                        currentMessageId = propValue;
                        returnValue.payload.set(currentMessageId, {});
                    }
                    else {
                        const payloadObject: any = returnValue.payload.get(currentMessageId) || {};
                        payloadObject[propName] = propValue;
                        returnValue.payload.set(currentMessageId, payloadObject);
                    }
                }
            }
            return returnValue;
        }
        finally {
            await this.client.release(token);
        }
    }

    public async acknowledge(batch: IBatchIdentity, dropBatch = true): Promise<boolean> {
        const token = `acknowledge-${Date.now().toString()}`;
        await this.client.acquire(token);
        try {
            await this.createStreamGroupIfNotExists(this.name, this.groupName, this.client, token);
            const response = await this.client.run(token, ["XACK", this.name, this.groupName, batch.id]);
            if (response === 1) {
                if (dropBatch === true) {
                    await this.client.run(token, ["XDEL", this.name, batch.id]);
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

    private async createStreamGroupIfNotExists(streamName: string, groupName: string, acquiredClient: IRedisClientPool, token: string, cache = this.groupsCreated): Promise<void> {
        if (cache.has(groupName)) {
            return;
        }
        try {
            await acquiredClient.run(token, ["XGROUP", "CREATE", streamName, groupName, "0", "MKSTREAM"]);
            cache.add(groupName);
        }
        catch (err: any) {
            if (err.message === 'BUSYGROUP Consumer Group name already exists') {
                cache.add(groupName);
            }
            else {
                throw err;
            }
        }
    }
} 