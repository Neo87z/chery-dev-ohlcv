import { createClient, RedisClientType } from 'redis';
import { Request, Response, NextFunction } from 'express';
import { logger } from '../config/logger';

export let redisClient: RedisClientType;

// Initialize Redis client
export async function initRedis() {
  if (!redisClient) {
    redisClient = createClient({
      url: 'redis://default:PK0K2ah4rPhNsu91FFK8WsiJdEd1DlhO@redis-13210.c92.us-east-1-3.ec2.redns.redis-cloud.com:13210',
      username: 'default',
      password: 'PK0K2ah4rPhNsu91FFK8WsiJdEd1DlhO',
      socket: {
        host: 'redis-13210.c92.us-east-1-3.ec2.redns.redis-cloud.com',
        port: 13210,
        reconnectStrategy: (retries) => Math.min(retries * 50, 3000),
        connectTimeout: 5000,
      },
      legacyMode: true,
    });

    redisClient.on('error', (err) => {
      logger.error('Redis client error:', err);
    });

    try {
      await redisClient.connect();
      logger.info('Connected to Redis');
    } catch (err) {
      logger.error('Failed to connect to Redis:', err);
      throw new Error('Redis connection failed');
    }
  }
  return redisClient;
}

// Invalidate a specific cache key
export const invalidateCache = async (cacheKey: string) => {
  try {
    const client = await initRedis();
    await client.del(cacheKey);
    logger.info(`Cache invalidated for: ${cacheKey}`);
  } catch (err) {
    logger.error('Error invalidating cache:', err);
  }
};

// Acquire a lock for preventing cache misses from hitting concurrently
export const acquireLock = async (cacheKey: string): Promise<boolean> => {
  const client = await initRedis();
  const lockKey = `lock:${cacheKey}`;
  const lock = await client.setNX(lockKey, 'locked');

  if (lock) {
    // Lock acquired, set expiration time
    await client.expire(lockKey, 30); // 30 seconds lock expiry
  }
  return lock;
};

// Release the lock
export const releaseLock = async (cacheKey: string) => {
  const client = await initRedis();
  const lockKey = `lock:${cacheKey}`;
  await client.del(lockKey);
};

// Helper function to set cache
export const setCacheData = async (cacheKey: string, data: any, ttl: number) => {
  try {
    const client = await initRedis();
    await client.setEx(cacheKey, ttl, JSON.stringify(data));
    logger.info(`Cache set for: ${cacheKey}`);
  } catch (err) {
    logger.error('Error setting cache:', err);
  }
};

// Cache middleware
export const cacheMiddleware = (ttl = 60) => {
  return async (req: Request, res: Response, next: NextFunction) => {
    if (req.method !== 'GET') {
      return next();
    }

    const cacheKey = `cache:${req.originalUrl}`;

    try {
      // Initialize Redis client
      const client = await initRedis();

      // Check if the cache exists
      const cachedData = await client.get(cacheKey);
      if (cachedData) {
        logger.info(`Cache hit for: ${cacheKey}`);
        return res.status(200).json(JSON.parse(cachedData));
      }

      // Check if cache population is in progress
      const inProgress = await client.get(`in-progress:${cacheKey}`);
      if (inProgress) {
        // Cache population is in progress, so do not proceed with the cache logic
        logger.info(`Cache population in progress for: ${cacheKey}`);
        return next();  // Continue with the flow, no caching involved
      }

      // Acquire lock to avoid cache miss hits concurrently
      const lockAcquired = await acquireLock(cacheKey);
      if (!lockAcquired) {
        // If lock can't be acquired, just continue with the flow (no caching)
        return next();
      }

      // Mark that cache population is in progress
      await client.setEx(`in-progress:${cacheKey}`, ttl, 'true');

      // Store the cache TTL on the request object for route handlers to use
      (req as any).cacheTTL = ttl;
      (req as any).cacheKey = cacheKey;

      // Wrap res.json to cache the response after sending it
      const originalJson = res.json;
      res.json = function (body) {
        originalJson.call(this, body); // Call original res.json

        // Cache response data with TTL
        client.setEx(cacheKey, ttl, JSON.stringify(body)).catch((err: any) => {
          logger.error(`Error caching response for ${cacheKey}:`, err);
        });

        // Release the lock and reset in-progress flag after setting the cache
        releaseLock(cacheKey);
        client.del(`in-progress:${cacheKey}`).catch((err: any) => {
          logger.error(`Error removing in-progress flag for ${cacheKey}:`, err);
        });

        return this;
      };

      return next();
    } catch (error) {
      logger.error('Cache middleware error:', error);
      return next(); // Proceed with the request in case of error
    }
  };
};

// Manual cache setter for routes that need explicit cache control
export const setCache = (req: Request, data: any) => {
  const cacheKey = (req as any).cacheKey;
  const ttl = (req as any).cacheTTL;
  console.log('fkkk222222')
  if (cacheKey && ttl) {
    setCacheData(cacheKey, data, ttl).catch(err => {
      logger.error(`Error manually setting cache for ${cacheKey}:`, err);
    });
  }
};