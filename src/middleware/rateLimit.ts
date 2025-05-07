import { Request, Response, NextFunction } from 'express';
import { RateLimiterRedis, RateLimiterMemory } from 'rate-limiter-flexible';
import { createClient, RedisClientType } from 'redis';
import { logger } from '../config/logger';

let rateLimiter: RateLimiterRedis | RateLimiterMemory;
let redisClient: RedisClientType | null = null;

async function setupRateLimiter() {
  if (!rateLimiter) {
    try {
      if (process.env.REDIS_URL) {
        if (!redisClient) {
          redisClient = createClient({
            url: process.env.REDIS_URL,
            socket: {
              reconnectStrategy: (retries) => Math.min(retries * 50, 3000)
            }
          });
          redisClient.on('error', (err) => {
            logger.error(`Redis error: ${err.message}`);
            rateLimiter = new RateLimiterMemory({
              points: 120,
              duration: 60,
            });
            logger.info('Switching to in-memory rate limiter due to Redis failure');
          });

          await redisClient.connect();
        }
        rateLimiter = new RateLimiterRedis({
          storeClient: redisClient,
          keyPrefix: 'rate_limit',
          points: 120, 
          duration: 60, 
        });
        
        logger.info('Redis rate limiter initialized');
      } else {
        rateLimiter = new RateLimiterMemory({
          points: 120,
          duration: 60,
        });

        logger.info('Memory rate limiter initialized (Redis URL not provided)');
      }
    } catch (error) {
      logger.error('Failed to initialize rate limiter, switching to memory store:', error);
      rateLimiter = new RateLimiterMemory({
        points: 120,
        duration: 60,
      });
    }
  }
  
  return rateLimiter;
}

export const rateLimitMiddleware = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const limiter = await setupRateLimiter();
    const key = req.ip || 'unknown-ip';
    await limiter.consume(key);
    next();
  } catch (error) {
    logger.warn(`Rate limit exceeded for IP: ${req.ip}`, {
      method: req.method,
      path: req.path,
      error: error instanceof Error ? error.message : String(error),
    });
    res.status(429).json({
      error: 'Too many requests',
      message: 'Rate limit exceeded, please try again later.',
    });
  }
};
