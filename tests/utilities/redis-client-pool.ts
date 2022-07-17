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