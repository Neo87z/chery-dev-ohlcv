import { Pool, PoolClient } from 'pg';
import { TokenInfo } from '../types/database';
import { logger } from '../config/logger';
import * as fs from 'fs';
import * as path from 'path';
import { setCache, getCache } from '../config/redis';
import Redis from 'ioredis';
import { LRUCache } from 'lru-cache';
import { Worker } from 'worker_threads';
import * as zlib from 'zlib';
import { promisify } from 'util';

const gzip = promisify(zlib.gzip);
const gunzip = promisify(zlib.gunzip);

// Primary Redis client for general commands
const redis = new Redis({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),
  retryStrategy: (times) => Math.min(times * 50, 2000),
});

// Dedicated Redis client for subscriptions
const redisSubscriber = new Redis({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),
  retryStrategy: (times) => Math.min(times * 50, 2000),
});

// In-memory LRU cache
const lruCache = new LRUCache<string, any>({
  max: 1000,
  ttl: 1000 * 60, // 1 minute
});

// Redis Lua script for atomic cache get/set
const luaGetSet = `
  local cached = redis.call('GET', KEYS[1])
  if cached then
    return cached
  end
  redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
  return nil
`;

// Cache warming for popular tokens
const warmCache = async () => {
  const popularTokens = ['0x123...', '0x456...']; // Replace with actual popular addresses
  try {
    for (const address of popularTokens) {
      const token = await redis.get(`token:${address}`);
      if (token) {
        lruCache.set(`token:${address}`, JSON.parse(token));
      }
    }
    logger.info('Cache warmed for popular tokens', { service: 'Cherry-TokenInfo' });
  } catch (error) {
    logger.error('Failed to warm cache:', { error, service: 'Cherry-TokenInfo' });
  }
};

const setupDbSchema = async (pool: Pool): Promise<void> => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Install extensions
    await client.query(`
      CREATE EXTENSION IF NOT EXISTS pg_trgm;
      CREATE EXTENSION IF NOT EXISTS btree_gin;
      CREATE EXTENSION IF NOT EXISTS pg_bigm;
      CREATE EXTENSION IF NOT EXISTS ivm;
    `);

    // Create tokens table with monthly partitioning
    await client.query(`
      CREATE TABLE IF NOT EXISTS tokens (
        id SERIAL PRIMARY KEY,
        contract_address VARCHAR(100) UNIQUE NOT NULL,
        create_date DATE NOT NULL,
        image_link TEXT,
        holders INTEGER DEFAULT 0,
        description TEXT CHECK (LENGTH(description) <= 5000),
        volume NUMERIC DEFAULT 0,
        tsv_description TSVECTOR GENERATED ALWAYS AS (
          setweight(to_tsvector('english', COALESCE(contract_address, '')), 'A') ||
          setweight(to_tsvector('english', COALESCE(description, '')), 'B')
        ) STORED,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      ) PARTITION BY RANGE (create_date);
    `);

    // Create yearly and monthly partitions
    await client.query(`
      CREATE TABLE IF NOT EXISTS tokens_2023 PARTITION OF tokens
        FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')
        PARTITION BY RANGE (create_date);
      CREATE TABLE IF NOT EXISTS tokens_2024 PARTITION OF tokens
        FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')
        PARTITION BY RANGE (create_date);
      CREATE TABLE IF NOT EXISTS tokens_2025 PARTITION OF tokens
        FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')
        PARTITION BY RANGE (create_date);

      -- Monthly partitions for 2024 and 2025
      CREATE TABLE IF NOT EXISTS tokens_2024_01 PARTITION OF tokens_2024
        FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
      CREATE TABLE IF NOT EXISTS tokens_2024_02 PARTITION OF tokens_2024
        FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
      CREATE TABLE IF NOT EXISTS tokens_2024_03 PARTITION OF tokens_2024
        FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
      CREATE TABLE IF NOT EXISTS tokens_2024_04 PARTITION OF tokens_2024
        FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');
      CREATE TABLE IF NOT EXISTS tokens_2024_05 PARTITION OF tokens_2024
        FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');
      CREATE TABLE IF NOT EXISTS tokens_2024_06 PARTITION OF tokens_2024
        FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');
      CREATE TABLE IF NOT EXISTS tokens_2024_07 PARTITION OF tokens_2024
        FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');
      CREATE TABLE IF NOT EXISTS tokens_2024_08 PARTITION OF tokens_2024
        FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');
      CREATE TABLE IF NOT EXISTS tokens_2024_09 PARTITION OF tokens_2024
        FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');
      CREATE TABLE IF NOT EXISTS tokens_2024_10 PARTITION OF tokens_2024
        FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');
      CREATE TABLE IF NOT EXISTS tokens_2024_11 PARTITION OF tokens_2024
        FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');
      CREATE TABLE IF NOT EXISTS tokens_2024_12 PARTITION OF tokens_2024
        FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');
      CREATE TABLE IF NOT EXISTS tokens_2025_01 PARTITION OF tokens_2025
        FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
      CREATE TABLE IF NOT EXISTS tokens_2025_02 PARTITION OF tokens_2025
        FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
      CREATE TABLE IF NOT EXISTS tokens_2025_03 PARTITION OF tokens_2025
        FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
      CREATE TABLE IF NOT EXISTS tokens_2025_04 PARTITION OF tokens_2025
        FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
      CREATE TABLE IF NOT EXISTS tokens_2025_05 PARTITION OF tokens_2025
        FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
    `);

    // Token makers table
    await client.query(`
      CREATE TABLE IF NOT EXISTS token_makers (
        id SERIAL PRIMARY KEY,
        token_id INTEGER REFERENCES tokens(id) ON DELETE CASCADE,
        maker_address VARCHAR(42) NOT NULL,
        UNIQUE(token_id, maker_address)
      );
    `);

    // Markets table
    await client.query(`
      CREATE TABLE IF NOT EXISTS markets (
        id SERIAL PRIMARY KEY,
        market_id VARCHAR(42) UNIQUE NOT NULL,
        quote_address VARCHAR(42) NOT NULL,
        base_address VARCHAR(42) NOT NULL,
        market_type VARCHAR(50) NOT NULL,
        market_data JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);

    // Create incremental materialized view
    await client.query(`
      CREATE INCREMENTAL MATERIALIZED VIEW IF NOT EXISTS tokens_search_view AS
      SELECT 
        t.id,
        t.contract_address,
        t.create_date,
        t.image_link,
        t.holders,
        t.description,
        t.volume,
        t.tsv_description,
        COALESCE((
          SELECT ARRAY_AGG(maker_address)
          FROM token_makers
          WHERE token_id = t.id
        ), '{}') as makers,
        ts_rank_cd(t.tsv_description, to_tsquery('english', '')) as rank,
        (SELECT COUNT(*) FROM token_makers WHERE token_id = t.id) as maker_count
      FROM tokens t
      WHERE t.create_date >= NOW() - INTERVAL '1 year'
      WITH DATA;
    `);

    // Optimized indexes
    await client.query(`
      -- Indexes on tokens table
      CREATE UNIQUE INDEX IF NOT EXISTS idx_tokens_contract_address ON tokens(contract_address);
      CREATE INDEX IF NOT EXISTS idx_tokens_tsv_description ON tokens USING GIN(tsv_description);
      CREATE INDEX IF NOT EXISTS idx_tokens_create_date ON tokens USING BRIN(create_date);
      CREATE INDEX IF NOT EXISTS idx_tokens_contract_address_bigram ON tokens USING GIN (contract_address bigm_ops);
      CREATE INDEX IF NOT EXISTS idx_tokens_description_bigram ON tokens USING GIN (description bigm_ops);
      CREATE INDEX IF NOT EXISTS idx_tokens_composite ON tokens (create_date, contract_address);

      -- Indexes on token_makers table
      CREATE INDEX IF NOT EXISTS idx_token_makers_token_id ON token_makers(token_id);
      CREATE INDEX IF NOT EXISTS idx_token_makers_maker_address ON token_makers(maker_address);

      -- Indexes on markets table
      CREATE INDEX IF NOT EXISTS idx_markets_base_address ON markets(base_address);
      CREATE INDEX IF NOT EXISTS idx_markets_quote_address ON markets(quote_address);
      CREATE INDEX IF NOT EXISTS idx_markets_market_data ON markets USING GIN (market_data);

      -- Indexes on materialized view
      CREATE UNIQUE INDEX IF NOT EXISTS idx_tokens_search_view_id ON tokens_search_view(id);
      CREATE INDEX IF NOT EXISTS idx_tokens_search_view_tsv ON tokens_search_view USING GIN(tsv_description);
      CREATE INDEX IF NOT EXISTS idx_tokens_search_view_contract ON tokens_search_view(contract_address);
      CREATE INDEX IF NOT EXISTS idx_tokens_search_view_create_date ON tokens_search_view USING BRIN(create_date);
    `);

    await client.query('COMMIT');
    logger.info('PostgreSQL schema and materialized view setup complete', { service: 'Cherry-TokenInfo' });
  } catch (error) {
    await client.query('ROLLBACK');
    logger.error('Error setting up PostgreSQL schema:', { error, service: 'Cherry-TokenInfo' });
    throw error;
  } finally {
    client.release();
  }
};

function isClickHouseClient(pool: any): boolean {
  return (
    (pool.constructor && pool.constructor.name === 'ClickHouseClient') ||
    (typeof pool.query === 'function' && typeof pool.connect !== 'function') ||
    (pool.client && pool.client.constructor && pool.client.constructor.name === 'ClickHouseClient') ||
    (typeof pool.insert === 'function' || typeof pool.stream === 'function')
  );
}

export class TokenInfoQueue {
  private static instance: TokenInfoQueue;
  private queue: TokenInfo[] = [];
  private isProcessing: boolean = false;
  private flushInterval: NodeJS.Timeout;
  private pool: Pool | null = null;
  private readonly FLUSH_THRESHOLD = 100;
  private readonly PERSISTENCE_FILE = path.join(process.cwd(), 'token_info_queue_backup.json.gz');
  private isClickHouse: boolean = false;
  private worker: Worker | null = null;

  private constructor() {
    this.flushInterval = setInterval(() => this.flushQueue(), 1000);
    this.loadPersistedTokens();
    setInterval(() => this.persistTokens(), 15000);
    setInterval(() => {
      const stats = this.getStats();
      logger.info(`Token queue status: ${stats.queueSize} tokens in queue, processing: ${stats.isProcessing}`, { service: 'Cherry-TokenInfo' });
    }, 5000);
    this.startWorker();
  }

  public static getInstance(): TokenInfoQueue {
    if (!TokenInfoQueue.instance) {
      TokenInfoQueue.instance = new TokenInfoQueue();
    }
    return TokenInfoQueue.instance;
  }

  private startWorker() {
    this.worker = new Worker(`
      const { parentPort } = require('worker_threads');
      parentPort.on('message', async (tokens) => {
        parentPort.postMessage({ processed: tokens.length });
      });
    `, { eval: true });
    this.worker.on('message', (msg) => {
      logger.info(`Worker processed ${msg.processed} tokens`, { service: 'Cherry-TokenInfo' });
    });
  }

  public setPool(pool: Pool) {
    this.pool = pool;
    this.isClickHouse = isClickHouseClient(pool);
    if (this.isClickHouse) {
      logger.info('ClickHouse client detected for token queue', { service: 'Cherry-TokenInfo' });
    } else {
      logger.info('PostgreSQL client detected for token queue', { service: 'Cherry-TokenInfo' });
      this.verifyDatabaseConnection().catch(error => {
        logger.error('Failed to verify database connection:', { error, service: 'Cherry-TokenInfo' });
      });
    }
  }

  private async verifyDatabaseConnection(): Promise<void> {
    if (!this.pool || this.isClickHouse) return;
    let client: PoolClient | null = null;
    try {
      client = await this.pool.connect();
      await client.query('SELECT 1');
      logger.info('Database connection verified', { service: 'Cherry-TokenInfo' });
    } catch (error) {
      logger.error('Database connection verification failed:', { error, service: 'Cherry-TokenInfo' });
    } finally {
      if (client) client.release();
    }
  }

  public addToQueue(token: TokenInfo) {
    if (!token.contract_address) {
      logger.warn('Attempted to add token with missing contract address to queue. Skipping.', { service: 'Cherry-TokenInfo' });
      return;
    }
    this.queue.push(token);
    if (this.queue.length >= this.FLUSH_THRESHOLD && !this.isProcessing) {
      this.flushQueue();
    }
  }

  public addManyToQueue(tokens: TokenInfo[]) {
    if (tokens.length === 0) return;
    const validTokens = tokens.filter(token => {
      if (!token.contract_address) {
        logger.warn('Filtered out token with missing contract address', { service: 'Cherry-TokenInfo' });
        return false;
      }
      return true;
    });
    this.queue.push(...validTokens);
    logger.info(`Added ${validTokens.length} tokens to queue (total size: ${this.queue.length})`, { service: 'Cherry-TokenInfo' });
    if (this.queue.length >= this.FLUSH_THRESHOLD && !this.isProcessing) {
      this.flushQueue();
    }
  }

  private async persistTokens() {
    if (this.queue.length === 0) return;
    try {
      const dataToSave = { queue: this.queue, timestamp: new Date().toISOString() };
      const dataString = JSON.stringify(dataToSave, null, 0);
      const compressed = await gzip(dataString);
      await fs.promises.writeFile(this.PERSISTENCE_FILE, compressed);
      logger.info(`Persisted ${this.queue.length} tokens to disk (compressed)`, { service: 'Cherry-TokenInfo' });
    } catch (error) {
      logger.error('Failed to persist token queue:', { error, service: 'Cherry-TokenInfo' });
    }
  }

  private async loadPersistedTokens() {
    try {
      if (fs.existsSync(this.PERSISTENCE_FILE)) {
        const compressed = await fs.promises.readFile(this.PERSISTENCE_FILE);
        const data = await gunzip(compressed);
        const parsed = JSON.parse(data.toString());
        if (parsed.queue && Array.isArray(parsed.queue)) {
          this.queue = parsed.queue;
          logger.info(`Loaded ${this.queue.length} persisted tokens from disk`, { service: 'Cherry-TokenInfo' });
          if (this.queue.length > 0) {
            setTimeout(() => this.flushQueue(), 1000);
          }
        }
      }
    } catch (error) {
      logger.error('Failed to load persisted tokens:', { error, service: 'Cherry-TokenInfo' });
    }
  }

  public async forceFlush(): Promise<boolean> {
    if (!this.pool || this.isClickHouse || this.isProcessing) {
      logger.error('Cannot force flush: Invalid state', { service: 'Cherry-TokenInfo' });
      return false;
    }
    await this.flushQueue();
    return true;
  }

  private async flushQueue(): Promise<void> {
    if (this.queue.length === 0 || this.isProcessing || !this.pool || this.isClickHouse) return;
    let client: PoolClient | null = null;
    let successful = 0;
    let failed = 0;

    try {
      this.isProcessing = true;
      logger.info(`Starting queue processing with ${this.queue.length} tokens`, { service: 'Cherry-TokenInfo' });
      client = await this.pool.connect();
      const tokensToProcess = [...this.queue];
      this.queue = [];
      await client.query('BEGIN');

      const addresses = tokensToProcess.map(t => t.contract_address);
      const { rows: existingTokens } = await client.query(
        `SELECT contract_address FROM tokens WHERE contract_address = ANY($1)`,
        [addresses]
      );

      const existingAddresses = new Set(existingTokens.map(item => item.contract_address));
      const newTokens = tokensToProcess.filter(t => !existingAddresses.has(t.contract_address));

      if (newTokens.length === 0) {
        await client.query('COMMIT');
        return;
      }

      // Batch insert tokens
      const tokenValues = newTokens.map(token => [
        token.contract_address,
        token.create_date ? new Date(token.create_date) : new Date(),
        token.image_link || null,
        token.holders || 0,
        (token.description || '').substring(0, 5000),
        token.volume || 0,
      ]);

      const tokenQuery = `
        INSERT INTO tokens (contract_address, create_date, image_link, holders, description, volume)
        SELECT * FROM unnest($1::varchar(100)[], $2::date[], $3::text[], $4::integer[], $5::text[], $6::numeric[])
        RETURNING id, contract_address
      `;
      const tokenResult = await client.query(tokenQuery, [
        tokenValues.map(v => v[0]),
        tokenValues.map(v => v[1]),
        tokenValues.map(v => v[2]),
        tokenValues.map(v => v[3]),
        tokenValues.map(v => v[4]),
        tokenValues.map(v => v[5]),
      ]);

      const tokenIds = tokenResult.rows.reduce((acc, row) => {
        acc[row.contract_address] = row.id;
        return acc;
      }, {} as Record<string, number>);

      // Batch insert token makers
      const makerValues: [number, string][] = [];
      for (const token of newTokens) {
        if (!token.contract_address || !tokenIds[token.contract_address]) continue;
        let makersArray: string[] = [];
        if (token.makers) {
          makersArray = Array.isArray(token.makers)
            ? token.makers.map(m => String(m))
            : typeof token.makers === 'string'
              ? token.makers.includes(',')
                ? token.makers.split(',').map(m => m.trim())
                : [token.makers]
              : [String(token.makers)];
        }
        for (const maker of makersArray) {
          if (maker) {
            makerValues.push([tokenIds[token.contract_address], maker]);
          }
        }
      }

      if (makerValues.length > 0) {
        await client.query(
          `
            INSERT INTO token_makers (token_id, maker_address)
            SELECT * FROM unnest($1::integer[], $2::varchar(42)[])
            ON CONFLICT DO NOTHING
          `,
          [makerValues.map(v => v[0]), makerValues.map(v => v[1])]
        );
      }

      successful = newTokens.length;
      await client.query('COMMIT');

      // Publish cache invalidation
      await redis.publish('token_updates', JSON.stringify(newTokens.map(t => t.contract_address)));
      logger.info(`Processed ${successful} tokens, ${failed} failed`, { service: 'Cherry-TokenInfo' });
    } catch (error) {
      logger.error('Error flushing queue:', { error, service: 'Cherry-TokenInfo' });
      if (client) await client.query('ROLLBACK');
      const tokensToProcess = [...this.queue];
      this.queue.push(...tokensToProcess);
      failed = tokensToProcess.length;
    } finally {
      if (client) client.release();
      this.isProcessing = false;
      if (this.queue.length > 0) setTimeout(() => this.flushQueue(), 200);
    }
  }

  public shutdown() {
    clearInterval(this.flushInterval);
    logger.info('Shutting down token queue', { service: 'Cherry-TokenInfo' });
    this.persistTokens();
    if (this.worker) {
      this.worker.terminate();
    }
    if (this.queue.length > 0 && this.pool && !this.isClickHouse) {
      return this.flushQueue();
    }
    return Promise.resolve();
  }

  public getStats() {
    return {
      queueSize: this.queue.length,
      isProcessing: this.isProcessing,
      isClickHouse: this.isClickHouse,
    };
  }
}

function setupShutdownHandlers(queue: TokenInfoQueue): void {
  process.on('SIGINT', async () => {
    await queue.shutdown();
    redis.quit();
    redisSubscriber.quit();
    process.exit(0);
  });
  process.on('SIGTERM', async () => {
    await queue.shutdown();
    redis.quit();
    redisSubscriber.quit();
    process.exit(0);
  });
}

export class TokenInfPostGresModel {
  private pool: Pool | any;
  private queue: TokenInfoQueue;
  private isClickHouse: boolean = false;

  constructor(pool: Pool | any) {
    this.pool = pool;
    this.isClickHouse = isClickHouseClient(pool);
    this.queue = TokenInfoQueue.getInstance();
    this.queue.setPool(pool);
    if (!this.isClickHouse) {
      this.testDatabaseAccess().catch(error => {
        logger.error('Initial database access test failed:', { error, service: 'Cherry-TokenInfo' });
      });
      warmCache();
      // Subscribe to cache invalidation using dedicated subscriber client
      redisSubscriber.subscribe('token_updates', (err, count) => {
        if (err) {
          logger.error('Failed to subscribe to token_updates:', { err, service: 'Cherry-TokenInfo' });
        } else {
          logger.info(`Subscribed to token_updates channel: ${count}`, { service: 'Cherry-TokenInfo' });
        }
      });
      redisSubscriber.on('message', (channel, message) => {
        if (channel === 'token_updates') {
          try {
            const addresses = JSON.parse(message);
            for (const address of addresses) {
              lruCache.delete(`token:${address}`);
              redis.del(`token:${address}`).catch(err => {
                logger.error(`Failed to delete cache for ${address}:`, { err, service: 'Cherry-TokenInfo' });
              });
            }
          } catch (error) {
            logger.error('Error processing token_updates message:', { error, service: 'Cherry-TokenInfo' });
          }
        }
      });
    }
  }

  private async testDatabaseAccess(): Promise<void> {
    if (this.isClickHouse) return;
    try {
      await this.pool.query('SELECT 1');
      logger.info('Database access test successful', { service: 'Cherry-TokenInfo' });
    } catch (error) {
      logger.error('Database access test failed:', { error, service: 'Cherry-TokenInfo' });
    }
  }

  async createTokenInfo(tokenData: TokenInfo): Promise<void> {
    if (!tokenData.contract_address) return;
    this.queue.addToQueue(tokenData);
  }

  async bulkCreateTokenInfos(tokens: TokenInfo[]): Promise<void> {
    this.queue.addManyToQueue(tokens);
  }

  async forceProcessQueue(): Promise<boolean> {
    if (this.isClickHouse) return false;
    return this.queue.forceFlush();
  }

  async findByAddress(address: string): Promise<TokenInfo | null> {
    if (this.isClickHouse) return null;
    try {
      const cacheKey = `token:${address}`;
      // Check LRU cache
      const lruCached = lruCache.get(cacheKey);
      if (lruCached) {
        return lruCached;
      }

      // Check Redis with Lua script
      const redisCached = await redis.eval(luaGetSet, 1, cacheKey, JSON.stringify({}), 30) as string | null;
      if (redisCached) {
        const parsed = JSON.parse(redisCached);
        lruCache.set(cacheKey, parsed);
        return parsed;
      }

      const query = {
        text: `
          SELECT contract_address, create_date, image_link, holders, description, volume, makers
          FROM tokens_search_view
          WHERE contract_address = $1
          LIMIT 1
        `,
        values: [address],
      };
      const result = await this.pool.query(query);
      if (result.rows.length === 0) return null;

      const token = result.rows[0];
      const tokenInfo: TokenInfo = {
        contract_address: token.contract_address,
        create_date: token.create_date.toISOString(),
        makers: token.makers || [],
        volume: token.volume.toString(),
        image_link: token.image_link,
        holders: token.holders,
        description: token.description,
      };

      // Cache in Redis and LRU
      await setCache(cacheKey, tokenInfo, 30);
      lruCache.set(cacheKey, tokenInfo);
      return tokenInfo;
    } catch (error) {
      logger.error('Error in findByAddress:', { error, service: 'Cherry-TokenInfo' });
      throw error;
    }
  }

  async findTokensByPage(page: number = 1, limit: number = 20): Promise<{ tokens: TokenInfo[]; total: number }> {
    if (this.isClickHouse) return { tokens: [], total: 0 };
    try {
      const cacheKey = `tokens:page:${page}:limit:${limit}`;
      const lruCached = lruCache.get(cacheKey);
      if (lruCached) {
        return lruCached;
      }

      const redisCached = await redis.eval(luaGetSet, 1, cacheKey, JSON.stringify({}), 30) as string | null;
      if (redisCached) {
        const parsed = JSON.parse(redisCached);
        lruCache.set(cacheKey, parsed);
        return parsed;
      }

      const offset = (page - 1) * limit;
      const query = {
        text: `
          SELECT contract_address, create_date, image_link, holders, description, volume, makers
          FROM tokens_search_view
          ORDER BY create_date DESC
          LIMIT $1 OFFSET $2
        `,
        values: [limit, offset],
      };
      const countQuery = { text: `SELECT COUNT(*) as total FROM tokens_search_view` };

      const [result, countResult] = await Promise.all([
        this.pool.query(query),
        this.pool.query(countQuery),
      ]);

      const tokens = result.rows.map((token: any) => ({
        contract_address: token.contract_address,
        create_date: token.create_date.toISOString(),
        makers: token.makers || [],
        volume: token.volume.toString(),
        image_link: token.image_link,
        holders: token.holders,
        description: token.description,
      }));

      const total = parseInt(countResult.rows[0].total);
      const response = { tokens, total };

      await setCache(cacheKey, response, 30);
      lruCache.set(cacheKey, response);
      return response;
    } catch (error) {
      logger.error('Error fetching tokens by page:', { error, service: 'Cherry-TokenInfo' });
      throw error;
    }
  }

  async updateTokenInfo(address: string, tokenData: Partial<TokenInfo>): Promise<boolean> {
    if (this.isClickHouse) return false;
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query(
        `SELECT id FROM tokens WHERE contract_address = $1`,
        [address]
      );
      if (rows.length === 0) {
        await client.query('ROLLBACK');
        return false;
      }

      const tokenId = rows[0].id;
      const updateFields = [];
      const updateValues = [];
      let valueCounter = 1;

      if (tokenData.create_date !== undefined) {
        updateFields.push(`create_date = $${valueCounter}`);
        updateValues.push(new Date(tokenData.create_date));
        valueCounter++;
      }
      if (tokenData.volume !== undefined) {
        updateFields.push(`volume = $${valueCounter}`);
        updateValues.push(tokenData.volume);
        valueCounter++;
      }
      if (tokenData.image_link !== undefined) {
        updateFields.push(`image_link = $${valueCounter}`);
        updateValues.push(tokenData.image_link);
        valueCounter++;
      }
      if (tokenData.holders !== undefined) {
        updateFields.push(`holders = $${valueCounter}`);
        updateValues.push(tokenData.holders);
        valueCounter++;
      }
      if (tokenData.description !== undefined) {
        updateFields.push(`description = $${valueCounter}`);
        updateValues.push(tokenData.description);
        valueCounter++;
      }

      if (updateFields.length > 0) {
        updateFields.push(`updated_at = NOW()`);
        await client.query(
          `UPDATE tokens SET ${updateFields.join(', ')} WHERE contract_address = $${valueCounter}`,
          [...updateValues, address]
        );
      }

      if (tokenData.makers && Array.isArray(tokenData.makers)) {
        await client.query(`DELETE FROM token_makers WHERE token_id = $1`, [tokenId]);
        if (tokenData.makers.length > 0) {
          const makerValues = tokenData.makers
            .filter(maker => maker)
            .map(maker => [tokenId, maker]);
          await client.query(
            `
              INSERT INTO token_makers (token_id, maker_address)
              SELECT * FROM unnest($1::integer[], $2::varchar(42)[])
              ON CONFLICT DO NOTHING
            `,
            [makerValues.map(v => v[0]), makerValues.map(v => v[1])]
          );
        }
      }

      await client.query('COMMIT');
      // Invalidate cache
      const cacheKey = `token:${address}`;
      lruCache.delete(cacheKey);
      await redis.del(cacheKey);
      await redis.publish('token_updates', JSON.stringify([address]));
      return true;
    } catch (error) {
      logger.error(`Error updating token ${address}:`, { error, service: 'Cherry-TokenInfo' });
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async searchTokens(searchText: string, limit: number = 20, offset: number = 0): Promise<{ tokens: TokenInfo[]; total: number }> {
    if (this.isClickHouse) return { tokens: [], total: 0 };
    try {
      const cacheKey = `search:${searchText}:${limit}:${offset}`;
      const lruCached = lruCache.get(cacheKey);
      if (lruCached) {
        return lruCached;
      }

      const redisCached = await redis.eval(luaGetSet, 1, cacheKey, JSON.stringify({}), 30) as string | null;
      if (redisCached) {
        const parsed = JSON.parse(redisCached);
        lruCache.set(cacheKey, parsed);
        return parsed;
      }

      const tsQuery = searchText
        .trim()
        .split(/\s+/)
        .filter(word => word.length > 0)
        .map(word => `${word}:*`)
        .join(' & ');

      if (!tsQuery) return { tokens: [], total: 0 };

      const query = {
        text: `
          SELECT contract_address, create_date, image_link, holders, description, volume, makers,
                 ts_rank_cd(tsv_description, to_tsquery('english', $1), 32) as rank
          FROM tokens_search_view
          WHERE tsv_description @@ to_tsquery('english', $1)
          ORDER BY rank DESC, create_date DESC
          LIMIT $2 OFFSET $3
        `,
        values: [tsQuery, limit, offset],
      };

      const countQuery = {
        text: `SELECT COUNT(*) as total FROM tokens_search_view WHERE tsv_description @@ to_tsquery('english', $1)`,
        values: [tsQuery],
      };

      const [results, countResult] = await Promise.all([
        this.pool.query(query),
        this.pool.query(countQuery),
      ]);

      const tokens = results.rows.map((token: any) => ({
        contract_address: token.contract_address,
        create_date: token.create_date.toISOString(),
        makers: token.makers || [],
        volume: token.volume.toString(),
        image_link: token.image_link,
        holders: token.holders,
        description: token.description,
      }));

      const total = parseInt(countResult.rows[0].total);
      const result = { tokens, total };

      await setCache(cacheKey, result, 30);
      lruCache.set(cacheKey, result);
      return result;
    } catch (error) {
      logger.error('Error searching tokens:', { error, service: 'Cherry-TokenInfo' });
      throw error;
    }
  }

  getQueueStats() {
    return this.queue.getStats();
  }

  static async initializeQueue(pool: Pool | any): Promise<void> {
    const isClickHouseDetected = isClickHouseClient(pool);
    logger.info(`Initializing token queue with ${isClickHouseDetected ? 'ClickHouse' : 'PostgreSQL'} client`, { service: 'Cherry-TokenInfo' });
    if (!isClickHouseDetected) {
      try {
        const testClient = await pool.connect();
        logger.info('Database connection test successful', { service: 'Cherry-TokenInfo' });
        testClient.release();
      } catch (error) {
        logger.error('Database connection test failed:', { error, service: 'Cherry-TokenInfo' });
        throw new Error('Failed to connect to database during initialization');
      }
      await setupDbSchema(pool);
    }
    const queue = TokenInfoQueue.getInstance();
    queue.setPool(pool);
    setupShutdownHandlers(queue);
  }
}