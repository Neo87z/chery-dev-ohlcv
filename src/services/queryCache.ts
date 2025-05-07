import { createClient, RedisClientType } from 'redis';
import { logger } from '../config/logger';

export class QueryCacheService {
  private static instance: QueryCacheService;
  private client: RedisClientType | null = null;
  private isConnecting: boolean = false;
  private connectionPromise: Promise<RedisClientType> | null = null;

  private constructor() {}

  static getInstance(): QueryCacheService {
    if (!QueryCacheService.instance) {
      QueryCacheService.instance = new QueryCacheService();
    }
    return QueryCacheService.instance;
  }

 
  async getClient(): Promise<RedisClientType> {
    if (this.client) {
      return this.client;
    }

    if (this.connectionPromise) {
      return this.connectionPromise;
    }

    this.connectionPromise = new Promise(async (resolve, reject) => {
      try {
        this.isConnecting = true;
        const client = createClient({
          url: process.env.REDIS_URL,
          socket: {
            reconnectStrategy: (retries) => Math.min(retries * 50, 3000),
          },
        }) as RedisClientType;

        client.on('error', (err) => {
          logger.error('Redis client error:', err);
        });

        await client.connect();
        this.client = client;
        logger.info('Redis connected successfully');
        resolve(client);
      } catch (error) {
        logger.error('Failed to connect to Redis:', error);
        reject(error);
      } finally {
        this.isConnecting = false;
        this.connectionPromise = null;
      }
    });

    return this.connectionPromise;
  }


  async get<T>(key: string): Promise<T | null> {
    try {
      const client = await this.getClient();
      const data = await client.get(key);

      if (!data) {
        logger.warn(`Cache miss for key: ${key}`);
        return null;
      }

      return JSON.parse(data) as T;
    } catch (error) {
      logger.error(`Redis get error for key: ${key}`, { error });
      return null;
    }
  }

 
  async set(key: string, value: any, ttl: number = 60): Promise<void> {
    try {
      const client = await this.getClient();
      await client.setEx(key, ttl, JSON.stringify(value));
      logger.info(`Cache set for key: ${key} with TTL: ${ttl}s`);
    } catch (error) {
      logger.error(`Redis set error for key: ${key}`, { error });
    }
  }


  async invalidate(pattern: string): Promise<void> {
    try {
      const client = await this.getClient();
      const keys = await client.keys(pattern);

      if (keys.length === 0) {
        logger.info(`No cache keys found to invalidate for pattern: ${pattern}`);
        return;
      }

      const multi = client.multi();
      keys.forEach((key) => multi.del(key)); 
      await multi.exec();
      logger.info(`Invalidated ${keys.length} cache keys matching pattern: ${pattern}`);
    } catch (error) {
      logger.error(`Redis invalidate error for pattern: ${pattern}`, { error });
    }
  }
}
