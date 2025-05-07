import Redis from 'ioredis';
import { logger } from './logger';

const redisConfig = {
  host: process.env.REDIS_HOST || 'redis-13210.c92.us-east-1-3.ec2.redns.redis-cloud.com',
  port: parseInt(process.env.REDIS_PORT || '13210'),
  password: process.env.REDIS_PASSWORD || 'PK0K2ah4rPhNsu91FFK8WsiJdEd1DlhO',
  username: 'default',
  db: parseInt(process.env.REDIS_DB || '0'),
  retryStrategy: (times: number) => Math.min(times * 50, 3000),
  connectTimeout: 5000,
  maxRetriesPerRequest: 3,
  enableReadyCheck: true,
};

const redis = new Redis(redisConfig);

redis.on('connect', () => {
  logger.info('Connected to Redis');
});

redis.on('error', (err) => {
  logger.error('Redis connection error:', err);
});

// Cache warming for popular tokens
const warmCache = async () => {
  try {
    const popularTokens = ['0x123...', '0x456...']; // Replace with actual popular token addresses
    const pipeline = redis.pipeline();
    for (const address of popularTokens) {
      pipeline.get(`token:${address}`);
    }
    await pipeline.exec();
    logger.info('Cache warmed for popular tokens');
  } catch (err) {
    logger.error('Error warming cache:', err);
  }
};

// Run cache warming on startup
warmCache();

export const setCache = async (key: string, value: any, expiry: number = 60) => {
  try {
    const pipeline = redis.pipeline();
    pipeline.set(key, JSON.stringify(value), 'EX', expiry);
    await pipeline.exec();
  } catch (err) {
    logger.error('Error setting cache:', err);
  }
};

export const getCache = async (key: string) => {
  try {
    const data = await redis.get(key);
    return data ? JSON.parse(data) : null;
  } catch (err) {
    logger.error('Error getting cache:', err);
    return null;
  }
};

export default redis;