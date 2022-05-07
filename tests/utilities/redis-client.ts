import { IRedisClient } from '../../source/index';
import ioredis from 'ioredis';
import fs from 'fs';
import Crypto from "crypto";

export class RedisClient implements IRedisClient {
    private redisClient: ioredis;
    private filenameToCommand = new Map<string, string>();

    constructor(redisConnectionString: string) {
        this.redisClient = new ioredis(redisConnectionString);
    }

    async shutdown(): Promise<void> {
        await this.redisClient.quit();
        this.redisClient.disconnect();
    }

    async acquire(token: string): Promise<void> {
        //console.time(token);
    }

    async release(token: string): Promise<void> {
        //console.timeEnd(token);
    }

    async run(commandArgs: string[]): Promise<any> {
        //console.log(commandArgs);
        const v = await this.redisClient.call(commandArgs.shift() as string, ...commandArgs);
        //console.log(v);
        return v;
    }

    async pipeline(commands: string[][]): Promise<any> {
        // console.log(commands);
        // @ts-expect-error
        const result = await this.redisClient.multi(commands)
            .exec();
        const finalResult = result?.map(r => {
            let err = r.shift();
            if (err != null) {
                throw err;
            }
            return r[0];
        });
        // console.log(finalResult);
        return finalResult;
    }

    async script(filename: string, keys: string[], args: string[]): Promise<any> {
        let command = this.filenameToCommand.get(filename);
        if (command == null) {
            const contents = await new Promise<string>((acc, rej) => {
                fs.readFile(filename, "utf8", (err, data) => {
                    if (err !== null) {
                        rej(err);
                    };
                    acc(data);
                });
            });
            command = Crypto.createHash("sha256").update(contents, "binary").digest("hex")
            this.redisClient.defineCommand(command, { lua: contents });
            this.filenameToCommand.set(filename, command);
        }
        // console.log(keys);
        // console.log(args);
        //console.log(filename);
        // @ts-ignore
        const results = await this.redisClient[command](keys.length, keys, args);
        //console.log(results);
        return results;
    }
}