import { Pool, PoolClient } from 'pg';
import { TokenInfo } from '../types/database';
import { logger } from '../config/logger';
import * as fs from 'fs';
import * as path from 'path';
import { setCache, getCache } from '../config/redis';

const setupDbSchema = async (pool: Pool): Promise<void> => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    logger.info('Starting PostgreSQL schema setup');

    // Install PostgreSQL extensions
    const extQuery = `
      CREATE EXTENSION IF NOT EXISTS pg_trgm;
      CREATE EXTENSION IF NOT EXISTS btree_gin;
    `;
    logger.debug('Executing query: Extensions setup\n' + extQuery);
    await client.query(extQuery);
    logger.info('Extensions pg_trgm and btree_gin installed');

    // Create tokens table
    const tokensQuery = `
      CREATE TABLE IF NOT EXISTS tokens (
        id SERIAL PRIMARY KEY,
        contract_address VARCHAR(100) UNIQUE NOT NULL,
        create_date TIMESTAMP NOT NULL,
        image_link TEXT,
        holders INTEGER DEFAULT 0,
        description TEXT,
        volume TEXT,
        tsv_description TSVECTOR GENERATED ALWAYS AS (
          setweight(to_tsvector('english', COALESCE(contract_address, '')), 'A') ||
          setweight(to_tsvector('english', COALESCE(description, '')), 'B')
        ) STORED,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `;
    logger.debug('Executing query: Create tokens table\n' + tokensQuery);
    await client.query(tokensQuery);
    logger.info('Tokens table created');

    // Create token_makers table
    const tokenMakersQuery = `
      CREATE TABLE IF NOT EXISTS token_makers (
        id SERIAL PRIMARY KEY,
        token_id INTEGER REFERENCES tokens(id) ON DELETE CASCADE,
        maker_address VARCHAR(100) NOT NULL,
        UNIQUE(token_id, maker_address)
      )
    `;
    logger.debug('Executing query: Create token_makers table\n' + tokenMakersQuery);
    await client.query(tokenMakersQuery);
    logger.info('Token_makers table created');

    // Create markets table
    const marketsQuery = `
      CREATE TABLE IF NOT EXISTS markets (
        id SERIAL PRIMARY KEY,
        market_id VARCHAR(42) UNIQUE NOT NULL,
        quote_address VARCHAR(100) NOT NULL,
        base_address VARCHAR(100) NOT NULL,
        market_type VARCHAR(50) NOT NULL,
        market_data JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `;
    logger.debug('Executing query: Create markets table\n' + marketsQuery);
    await client.query(marketsQuery);
    logger.info('Markets table created');

    // Create simplified materialized view for search
    const materializedViewQuery = `
      CREATE MATERIALIZED VIEW IF NOT EXISTS tokens_search_view AS
      SELECT 
        t.id,
        t.contract_address,
        t.create_date,
        t.image_link,
        t.holders,
        t.description,
        t.volume,
        t.tsv_description
      FROM tokens t
      WITH DATA;
    `;
    logger.debug('Executing query: Create tokens_search_view\n' + materializedViewQuery);
    await client.query(materializedViewQuery);
    logger.info('Tokens_search_view materialized view created');

    // Create optimized indexes
    const indexesQuery = `
      CREATE UNIQUE INDEX IF NOT EXISTS idx_tokens_search_view_id ON tokens_search_view(id);
      CREATE INDEX IF NOT EXISTS idx_tokens_search_view_tsv ON tokens_search_view USING GIN(tsv_description);
      CREATE INDEX IF NOT EXISTS idx_tokens_search_view_contract ON tokens_search_view(contract_address);
      CREATE INDEX IF NOT EXISTS idx_tokens_contract_address ON tokens(contract_address);
      CREATE INDEX IF NOT EXISTS idx_token_makers_token_id ON token_makers(token_id);
      CREATE INDEX IF NOT EXISTS idx_token_makers_maker_address ON token_makers(maker_address);
      CREATE INDEX IF NOT EXISTS idx_markets_base_address ON markets(base_address);
      CREATE INDEX IF NOT EXISTS idx_markets_quote_address ON markets(quote_address);
      CREATE INDEX IF NOT EXISTS idx_markets_market_data ON markets USING GIN(market_data);
      CREATE INDEX IF NOT EXISTS idx_tokens_contract_address_trgm ON tokens USING GIN(contract_address gin_trgm_ops);
      CREATE INDEX IF NOT EXISTS idx_tokens_description_trgm ON tokens USING GIN(description gin_trgm_ops);
    `;
    logger.debug('Executing query: Create indexes\n' + indexesQuery);
    await client.query(indexesQuery);
    logger.info('Indexes created');

    // Create function to refresh materialized view
    const refreshFunctionQuery = `
    CREATE OR REPLACE FUNCTION refresh_tokens_search_view()
RETURNS void AS $$
BEGIN
  -- Refresh the materialized view if it exists
  IF EXISTS (
    SELECT 1
    FROM pg_matviews
    WHERE matviewname = 'tokens_search_view'
  ) THEN
    EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY tokens_search_view';
  END IF;
EXCEPTION
  WHEN others THEN
    RAISE NOTICE 'Error refreshing tokens_search_view: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

    `;
    logger.debug('Executing query: Create refresh function\n' + refreshFunctionQuery);
    await client.query(refreshFunctionQuery);
    logger.info('Refresh function for tokens_search_view created');

    await client.query('COMMIT');
    logger.info('PostgreSQL schema and materialized view setup completed successfully');
  } catch (error) {
    await client.query('ROLLBACK');
    logger.error('Error setting up PostgreSQL schema:', error);
    throw error;
  } finally {
    client.release();
    logger.info('Database client released');
  }
};

// Function to schedule materialized view refresh (e.g., via pg_cron or external scheduler)
const scheduleMaterializedViewRefresh = async (pool: Pool): Promise<void> => {
  try {
    // Assuming pg_cron is installed, schedule refresh every 5 minutes
    const cronQuery = `
      SELECT cron.schedule(
        'refresh_tokens_search_view',
        '5 minutes',
        $$SELECT refresh_tokens_search_view();$$
      );
    `;
    logger.debug('Executing query: Schedule materialized view refresh');
    await pool.query(cronQuery);
    logger.info('Scheduled materialized view refresh every 5 minutes');
  } catch (error) {
    logger.error('Error scheduling materialized view refresh:', error);
    throw error;
  }
};

export { setupDbSchema, scheduleMaterializedViewRefresh };

function isClickHouseClient(pool: any): boolean {
  return (
    (pool.constructor && pool.constructor.name === 'ClickHouseClient') ||
    (typeof pool.query === 'function' && typeof pool.connect !== 'function') ||
    (pool.client && pool.client.constructor && 
     pool.client.constructor.name === 'ClickHouseClient') ||
    (typeof pool.insert === 'function' || typeof pool.stream === 'function')
  );
}

export class TokenInfoQueue {
  private static instance: TokenInfoQueue;
  private queue: TokenInfo[] = [];
  private isProcessing: boolean = false;
  private flushInterval: NodeJS.Timeout;
  private pool: Pool | null = null;
  private readonly FLUSH_THRESHOLD = 5;
  private readonly PERSISTENCE_FILE = path.join(process.cwd(), 'token_info_queue_backup.json');
  private isClickHouse: boolean = false;

  private constructor() {
    this.flushInterval = setInterval(() => this.flushQueue(), 3000);
    this.loadPersistedTokens();
    setInterval(() => this.persistTokens(), 30000);
    setInterval(() => {
      const stats = this.getStats();
      logger.info(`Token queue status: ${stats.queueSize} tokens in queue, processing: ${stats.isProcessing}`);
    }, 10000);
  }

  public static getInstance(): TokenInfoQueue {
    if (!TokenInfoQueue.instance) {
      TokenInfoQueue.instance = new TokenInfoQueue();
    }
    return TokenInfoQueue.instance;
  }

  public setPool(pool: Pool) {
    this.pool = pool;
    this.isClickHouse = isClickHouseClient(pool);
    if (this.isClickHouse) {
      logger.info('ClickHouse client detected for token queue');
    } else {
      logger.info('PostgreSQL client detected for token queue');
      this.verifyDatabaseConnection().catch(error => {
        logger.error('Failed to verify database connection:', error);
      });
    }
  }

  private async verifyDatabaseConnection(): Promise<void> {
    if (!this.pool || this.isClickHouse) return;
    let client: PoolClient | null = null;
    try {
      client = await this.pool.connect();
      const result = await client.query('SELECT COUNT(*) FROM tokens');
      logger.info(`Database connection verified. Current token count: ${result.rows[0].count}`);
    } catch (error) {
      logger.error('Database connection verification failed:', error);
    } finally {
      if (client) client.release();
    }
  }

  public addToQueue(token: TokenInfo) {
    if (!token.contract_address) {
      logger.warn('Attempted to add token with missing contract address to queue. Skipping.');
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
        logger.warn('Filtered out token with missing contract address');
        return false;
      }
      return true;
    });
    this.queue.push(...validTokens);
    logger.info(`Added ${validTokens.length} tokens to queue (total size: ${this.queue.length})`);
    if (this.queue.length >= this.FLUSH_THRESHOLD && !this.isProcessing) {
      this.flushQueue();
    }
  }

  private async persistTokens() {
    if (this.queue.length === 0) return;
    try {
      const dataToSave = { queue: this.queue, timestamp: new Date().toISOString() };
      await fs.promises.writeFile(this.PERSISTENCE_FILE, JSON.stringify(dataToSave), 'utf8');
      logger.info(`Persisted ${this.queue.length} tokens to disk`);
    } catch (error) {
      logger.error('Failed to persist token queue:', { error, service: "Cherry-TokenInfo" });
    }
  }

  private async loadPersistedTokens() {
    try {
      if (fs.existsSync(this.PERSISTENCE_FILE)) {
        const data = await fs.promises.readFile(this.PERSISTENCE_FILE, 'utf8');
        const parsed = JSON.parse(data);
        if (parsed.queue && Array.isArray(parsed.queue)) {
          this.queue = parsed.queue;
          logger.info(`Loaded ${this.queue.length} persisted tokens from disk`);
          if (this.queue.length > 0) {
            logger.info('Scheduling processing of persisted tokens');
            setTimeout(() => this.flushQueue(), 2000);
          }
        }
      }
    } catch (error) {
      logger.error('Failed to load persisted tokens:', { error, service: "Cherry-TokenInfo" });
    }
  }

  public async forceFlush(): Promise<boolean> {
    if (!this.pool || this.isClickHouse || this.isProcessing) {
      logger.error('Cannot force flush: Invalid state');
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
      logger.info(`Starting queue processing with ${this.queue.length} tokens`);
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

      for (const token of newTokens) {
        if (!token.contract_address) {
          failed++;
          continue;
        }

        try {
          const tokenResult = await client.query(
            `INSERT INTO tokens(
              contract_address, create_date, image_link, holders, description, volume
            ) VALUES($1, $2, $3, $4, $5, $6) RETURNING id`,
            [
              token.contract_address,
              token.create_date ? new Date(token.create_date) : new Date(),
              token.image_link || null,
              token.holders || 0,
              (token.description || '').substring(0, 10000),
              token.volume || '0'
            ]
          );

          const tokenId = tokenResult.rows[0].id;
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

          if (makersArray.length > 0) {
            for (const maker of makersArray) {
              if (!maker) continue;
              await client.query(
                `INSERT INTO token_makers(token_id, maker_address) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
                [tokenId, maker]
              );
            }
          }

          successful++;
        } catch (tokenError) {
          logger.error(`Failed to insert token ${token.contract_address}:`, tokenError);
          failed++;
        }
      }

      await client.query('COMMIT');
      await client.query('REFRESH MATERIALIZED VIEW CONCURRENTLY tokens_search_view');
      logger.info(`Processed ${successful} tokens, ${failed} failed`);
    } catch (error) {
      logger.error('Error flushing queue:', error);
      if (client) await client.query('ROLLBACK');
      const tokensToProcess = [...this.queue];
      this.queue = [...tokensToProcess, ...this.queue];
    } finally {
      if (client) client.release();
      this.isProcessing = false;
      if (this.queue.length > 0) setTimeout(() => this.flushQueue(), 1000);
    }
  }

  public shutdown() {
    clearInterval(this.flushInterval);
    logger.info('Shutting down token queue');
    this.persistTokens();
    if (this.queue.length > 0 && this.pool && !this.isClickHouse) {
      return this.flushQueue();
    }
    return Promise.resolve();
  }

  public getStats() {
    return {
      queueSize: this.queue.length,
      isProcessing: this.isProcessing,
      isClickHouse: this.isClickHouse
    };
  }
}

function setupShutdownHandlers(queue: TokenInfoQueue): void {
  process.on('SIGINT', async () => {
    await queue.shutdown();
    process.exit(0);
  });
  process.on('SIGTERM', async () => {
    await queue.shutdown();
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
        logger.error('Initial database access test failed:', error);
      });
    }
  }

  private async testDatabaseAccess(): Promise<void> {
    if (this.isClickHouse) return;
    try {
      const { rows } = await this.pool.query('SELECT COUNT(*) FROM tokens');
      logger.info(`Database initialized with ${rows[0].count} existing tokens`);
    } catch (error) {
      logger.error('Database access test failed:', error);
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
      const query = {
        text: `
          SELECT * FROM tokens_search_view WHERE contract_address = $1 LIMIT 1
        `,
        values: [address]
      };
      const result = await this.pool.query(query);
      if (result.rows.length === 0) return null;
      const token = result.rows[0];
      return {
        contract_address: token.contract_address,
        create_date: token.create_date.toISOString(),
        makers: token.makers || [],
        volume: token.volume,
        image_link: token.image_link,
        holders: token.holders,
        description: token.description
      };
    } catch (error) {
      logger.error('Error in findByAddress:', error);
      throw error;
    }
  }

  async findTokensByPage(page: number = 1, limit: number = 20): Promise<{tokens: TokenInfo[], total: number}> {
    if (this.isClickHouse) return { tokens: [], total: 0 };
    try {
      const offset = (page - 1) * limit;
      const query = {
        text: `
          SELECT * FROM tokens_search_view
          ORDER BY create_date DESC
          LIMIT $1 OFFSET $2
        `,
        values: [limit, offset]
      };
      const countQuery = { text: `SELECT COUNT(*) as total FROM tokens_search_view` };
      const [result, countResult] = await Promise.all([
        this.pool.query(query),
        this.pool.query(countQuery)
      ]);
      const tokens = result.rows.map((token: { contract_address: any; create_date: { toISOString: () => any; }; makers: any; volume: any; image_link: any; holders: any; description: any; }) => ({
        contract_address: token.contract_address,
        create_date: token.create_date.toISOString(),
        makers: token.makers || [],
        volume: token.volume,
        image_link: token.image_link,
        holders: token.holders,
        description: token.description
      }));
      const total = parseInt(countResult.rows[0].total);
      return { tokens, total };
    } catch (error) {
      logger.error('Error fetching tokens by page:', error);
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
          for (const maker of tokenData.makers) {
            if (!maker) continue;
            await client.query(
              `INSERT INTO token_makers(token_id, maker_address) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
              [tokenId, maker]
            );
          }
        }
      }

      await client.query('COMMIT');
      await client.query('REFRESH MATERIALIZED VIEW CONCURRENTLY tokens_search_view');
      return true;
    } catch (error) {
      logger.error(`Error updating token ${address}:`, error);
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async searchTokens(searchText: string, limit: number = 20, offset: number = 0): Promise<{tokens: TokenInfo[], total: number}> {
    if (this.isClickHouse) return { tokens: [], total: 0 };
    try {
      const cacheKey = `search:${searchText}:${limit}:${offset}`;
      const cached = await getCache(cacheKey);
      if (cached) {
        logger.info('Returning cached search results');
        return cached;
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
          SELECT id, contract_address, create_date, image_link, holders, description, volume, makers,
                 ts_rank(tsv_description, to_tsquery('english', $1)) as rank
          FROM tokens_search_view
          WHERE tsv_description @@ to_tsquery('english', $1)
          ORDER BY rank DESC
          LIMIT $2 OFFSET $3
        `,
        values: [tsQuery, limit, offset]
      };

      const countQuery = {
        text: `
          SELECT COUNT(*) as total
          FROM tokens_search_view
          WHERE tsv_description @@ to_tsquery('english', $1)
        `,
        values: [tsQuery]
      };

      const [results, countResult] = await Promise.all([
        this.pool.query(query),
        this.pool.query(countQuery)
      ]);

      const tokens = results.rows.map((token: { contract_address: any; create_date: { toISOString: () => any; }; makers: any; volume: any; image_link: any; holders: any; description: any; }) => ({
        contract_address: token.contract_address,
        create_date: token.create_date.toISOString(),
        makers: token.makers || [],
        volume: token.volume,
        image_link: token.image_link,
        holders: token.holders,
        description: token.description
      }));

      const total = parseInt(countResult.rows[0].total);
      const result = { tokens, total };

      await setCache(cacheKey, result);
      return result;
    } catch (error) {
      logger.error('Error searching tokens:', error);
      throw error;
    }
  }

  getQueueStats() {
    return this.queue.getStats();
  }

  static async initializeQueue(pool: Pool | any): Promise<void> {
    const isClickHouseDetected = isClickHouseClient(pool);
    logger.info(`Initializing token queue with ${isClickHouseDetected ? 'ClickHouse' : 'PostgreSQL'} client`);
    if (!isClickHouseDetected) {
      try {
        const testClient = await pool.connect();
        logger.info('Database connection test successful');
        testClient.release();
      } catch (error) {
        logger.error('Database connection test failed:', error);
        throw new Error('Failed to connect to database during initialization');
      }
      await setupDbSchema(pool);
    }
    const queue = TokenInfoQueue.getInstance();
    queue.setPool(pool);
    setupShutdownHandlers(queue);
  }
}

