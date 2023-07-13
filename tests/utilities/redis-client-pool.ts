import { IRedisClientPool } from '../../source/index';
import { randomUUID, createHash } from 'node:crypto';
import Redis, { Cluster } from 'ioredis';
import { parseURL } from 'ioredis/built/utils';
import fs from 'fs';
export type RedisConnection = Cluster | Redis;

export function createInstance<T>(c: new (...args: any) => T, args: any[] = []): T { return new c(...args); }
export interface IDispose { destroy(): Promise<void>; }


export class RedisClientPool implements IRedisClientPool, IDispose {
    private poolRedisClients: RedisConnection[];
    private activeRedisClients: Map<string, RedisConnection>;
    private filenameToCommand = new Map<string, string>();
    private redisConnectionCreator: () => RedisConnection;
    private idlePoolSize: number;
    private totalConnectionCounter = 0;

    constructor(redisConnectionCreator: () => RedisConnection, idlePoolSize = 6) {
        this.poolRedisClients = Array.from({ length: idlePoolSize }, (_) => redisConnectionCreator());
        this.totalConnectionCounter += idlePoolSize;
        this.activeRedisClients = new Map<string, RedisConnection>();
        this.redisConnectionCreator = redisConnectionCreator;
        this.idlePoolSize = idlePoolSize;
    }

    public generateUniqueToken(prefix: string) {
        return `${prefix}-${randomUUID()}`;
    }

    public async destroy(): Promise<void> {
        await this.shutdown();
    }

    public async shutdown() {
        const waitHandles = [...this.poolRedisClients, ...Array.from(this.activeRedisClients.values())]
            .map(async _ => { await _.quit(); _.disconnect(); });
        await Promise.allSettled(waitHandles);

        this.poolRedisClients = [];
        this.activeRedisClients.clear();
        this.totalConnectionCounter = 0;
    }

    public async acquire(token: string) {
        if (!this.activeRedisClients.has(token)) {
            const availableClient = this.poolRedisClients.pop() || (() => { this.totalConnectionCounter += 1; return this.redisConnectionCreator(); })();
            this.activeRedisClients.set(token, availableClient);
        }
    }

    async release(token: string) {
        const releasedClient = this.activeRedisClients.get(token);
        if (releasedClient == undefined) {
            return;
        }
        this.activeRedisClients.delete(token);
        if (this.poolRedisClients.length < this.idlePoolSize) {
            this.poolRedisClients.push(releasedClient);
        }
        else {
            await releasedClient.quit();
            releasedClient.disconnect();
        }
    }

    public async run(token: string, commandArgs: any) {
        const redisClient = this.activeRedisClients.get(token);
        if (redisClient == undefined) {
            throw new Error("Please acquire a client with proper token");
        }
        return await redisClient.call(commandArgs.shift(), ...commandArgs);
    }

    public async pipeline(token: string, commands: string[][], transaction = true) {
        const redisClient = this.activeRedisClients.get(token);
        if (redisClient == undefined) {
            throw new Error("Please acquire a client with proper token");
        }
        const result = transaction === true ? await redisClient.multi(commands).exec() : await redisClient.pipeline(commands).exec();
        return result?.map(r => {
            let err = r[0];
            if (err != null) {
                throw err;
            }
            return r[1];
        });
    }

    public async script(token: string, filename: string, keys: string[], args: any[]) {
        const redisClient = this.activeRedisClients.get(token);
        if (redisClient == undefined) {
            throw new Error("Please acquire a client with proper token");
        }
        let command = this.filenameToCommand.get(filename);
        // @ts-ignore
        if (command == null || redisClient[command] == null) {
            const contents = await fs.promises.readFile(filename, { encoding: "utf-8" });
            command = this.MD5Hash(contents);
            redisClient.defineCommand(command, { lua: contents });
            this.filenameToCommand.set(filename, command);
        }
        // @ts-ignore
        return await redisClient[command](keys.length, keys, args);
    }

    public async defineServerLuaCommand(token: string, contents: string): Promise<string> {
        const redisClient = this.activeRedisClients.get(token);
        if (redisClient == undefined) {
            throw new Error("Please acquire a client with proper token");
        }
        const commandName = this.MD5Hash(contents);
        let serverCommandHash = this.filenameToCommand.get(commandName);
        if (serverCommandHash != null) {
            return serverCommandHash;
            // Is this part needed for replication or cluster mode of redis? need to test before we enable below
            // const existsReply = await redisClient.script("EXISTS", serverCommandHash) as number[];
            // if (existsReply[0] == 1) {
            //     return serverCommandHash;
            // }
        }
        serverCommandHash = await redisClient.script("LOAD", contents) as string;
        this.filenameToCommand.set(commandName, serverCommandHash);
        return serverCommandHash;
    }

    public info() {
        const returnObj = {
            "Idle Size": this.idlePoolSize,
            "Current Active": this.activeRedisClients.size,
            "Pooled Connection": this.poolRedisClients.length,
            "Peak Connections": this.totalConnectionCounter
        };
        this.totalConnectionCounter = 0;
        return returnObj;
    }

    private MD5Hash(value: string): string {
        return createHash('md5').update(value).digest('hex');
    }
}

export function RedisClientClusterFactory(connectionDetails: string[], instanceInjection: <T>(c: new (...args: any) => T, args: any[]) => T = createInstance): RedisConnection {
    const distinctConnections = new Set<string>(connectionDetails);
    if (distinctConnections.size === 0) {
        throw new Error("Inncorrect or Invalid Connection details, cannot be empty");
    }

    if (connectionDetails.length > distinctConnections.size || distinctConnections.size > 1) {
        const parsedRedisURl = parseURL(connectionDetails[0]);//Assuming all have same password(they should have finally its a cluster)
        const awsElasticCacheOptions = {
            dnsLookup: (address: string, callback: any) => callback(null, address),
            redisOptions: {
                tls: connectionDetails[0].startsWith("rediss:") == true ? {} : undefined,
                password: parsedRedisURl.password as string | undefined,
                maxRedirections: 32
            },
        }
        return instanceInjection<Cluster>(Cluster, [Array.from(distinctConnections.values()), awsElasticCacheOptions]);
    }
    else {
        return instanceInjection<Redis>(Redis, [connectionDetails[0]]);
    }
}