import { Pool, PoolClient } from 'pg';
import { SocialInfo } from '../types/database';
import { logger } from '../config/logger';
import * as fs from 'fs';
import * as path from 'path';
import { setCache, getCache } from '../config/redis';

const setupSocialDbSchema = async (pool: Pool): Promise<void> => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    logger.info('Starting PostgreSQL schema setup for social_info');

    // Install PostgreSQL extensions
    const extQuery = `
      CREATE EXTENSION IF NOT EXISTS pg_trgm;
      CREATE EXTENSION IF NOT EXISTS btree_gin;
    `;
    logger.debug('Executing query: Extensions setup\n' + extQuery);
    await client.query(extQuery);
    logger.info('Extensions pg_trgm and btree_gin installed');

    // Create social_info table
    const socialInfoQuery = `
      CREATE TABLE IF NOT EXISTS social_info (
        id SERIAL PRIMARY KEY,
        contract_address VARCHAR(100) UNIQUE NOT NULL,
        website VARCHAR(255),
        telegram VARCHAR(255),
        twitter VARCHAR(255),
        github VARCHAR(255),
        explorer VARCHAR(255),
        other_social JSONB NOT NULL DEFAULT '[]',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        tsv_search TSVECTOR GENERATED ALWAYS AS (
          setweight(to_tsvector('english', COALESCE(contract_address, '')), 'A') ||
          setweight(to_tsvector('english', COALESCE(website, '')), 'B') ||
          setweight(to_tsvector('english', COALESCE(telegram, '')), 'B') ||
          setweight(to_tsvector('english', COALESCE(twitter, '')), 'B')
        ) STORED
      )
    `;
    logger.debug('Executing query: Create social_info table\n' + socialInfoQuery);
    await client.query(socialInfoQuery);
    logger.info('Social_info table created');

    // Create materialized view for search
    const materializedViewQuery = `
      CREATE MATERIALIZED VIEW IF NOT EXISTS social_info_search_view AS
      SELECT 
        id,
        contract_address,
        website,
        telegram,
        twitter,
        github,
        explorer,
        other_social,
        created_at,
        updated_at,
        tsv_search
      FROM social_info
      WITH DATA;
    `;
    logger.debug('Executing query: Create social_info_search_view\n' + materializedViewQuery);
    await client.query(materializedViewQuery);
    logger.info('Social_info_search_view materialized view created');

    // Create optimized indexes
    const indexesQuery = `
      CREATE UNIQUE INDEX IF NOT EXISTS idx_social_info_search_view_id ON social_info_search_view(id);
      CREATE INDEX IF NOT EXISTS idx_social_info_search_view_tsv ON social_info_search_view USING GIN(tsv_search);
      CREATE INDEX IF NOT EXISTS idx_social_info_search_view_contract ON social_info_search_view(contract_address);
      CREATE INDEX IF NOT EXISTS idx_social_info_contract_address ON social_info(contract_address);
      CREATE INDEX IF NOT EXISTS idx_social_info_website ON social_info(website);
      CREATE INDEX IF NOT EXISTS idx_social_info_telegram ON social_info(telegram);
      CREATE INDEX IF NOT EXISTS idx_social_info_twitter ON social_info(twitter);
      CREATE INDEX IF NOT EXISTS idx_social_info_contract_address_trgm ON social_info USING GIN(contract_address gin_trgm_ops);
      CREATE INDEX IF NOT EXISTS idx_social_info_website_trgm ON social_info USING GIN(website gin_trgm_ops);
      CREATE INDEX IF NOT EXISTS idx_social_info_telegram_trgm ON social_info USING GIN(telegram gin_trgm_ops);
      CREATE INDEX IF NOT EXISTS idx_social_info_twitter_trgm ON social_info USING GIN(twitter gin_trgm_ops);
    `;
    logger.debug('Executing query: Create indexes\n' + indexesQuery);
    await client.query(indexesQuery);
    logger.info('Indexes created');

    // Create function to refresh materialized view
    const refreshFunctionQuery = `
      CREATE OR REPLACE FUNCTION refresh_social_info_search_view()
      RETURNS void AS $$
      BEGIN
        IF EXISTS (
          SELECT 1
          FROM pg_matviews
          WHERE matviewname = 'social_info_search_view'
        ) THEN
          EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY social_info_search_view';
        END IF;
      EXCEPTION
        WHEN others THEN
          RAISE NOTICE 'Error refreshing social_info_search_view: %', SQLERRM;
      END;
      $$ LANGUAGE plpgsql;
    `;
    logger.debug('Executing query: Create refresh function\n' + refreshFunctionQuery);
    await client.query(refreshFunctionQuery);
    logger.info('Refresh function for social_info_search_view created');

    await client.query('COMMIT');
    logger.info('PostgreSQL social_info schema and materialized view setup completed successfully');
  } catch (error) {
    await client.query('ROLLBACK');
    logger.error('Error setting up PostgreSQL social_info schema:', error);
    throw error;
  } finally {
    client.release();
    logger.info('Database client released');
  }
};

// Start application-level materialized view refresh
const startSocialInfoMaterializedViewRefresh = (pool: Pool): NodeJS.Timeout => {
  const interval = setInterval(async () => {
    try {
      await pool.query('REFRESH MATERIALIZED VIEW CONCURRENTLY social_info_search_view');
      logger.info('Social_info materialized view refreshed by application');
    } catch (error) {
      logger.error('Error refreshing social_info materialized view in application:', error);
    }
  }, 5 * 60 * 1000); // Refresh every 5 minutes
  return interval;
};

export class SocialInfoQueue {
  private static instance: SocialInfoQueue;
  private queue: SocialInfo[] = [];
  private isProcessing: boolean = false;
  private flushInterval: NodeJS.Timeout;
  private pool: Pool | null = null;
  private readonly FLUSH_THRESHOLD = 10;
  private readonly PERSISTENCE_FILE = path.join(process.cwd(), 'social_info_queue_backup.json');

  private constructor() {
    this.flushInterval = setInterval(() => this.flushQueue(), 5000);
    this.loadPersistedSocials();
    setInterval(() => this.persistSocials(), 60000);
    setInterval(() => {
      const stats = this.getStats();
      logger.info(`Social info queue status: ${stats.queueSize} social infos in queue, processing: ${stats.isProcessing}`);
    }, 10000);
  }

  public static getInstance(): SocialInfoQueue {
    if (!SocialInfoQueue.instance) {
      SocialInfoQueue.instance = new SocialInfoQueue();
    }
    return SocialInfoQueue.instance;
  }

  public setPool(pool: Pool) {
    this.pool = pool;
    logger.info('PostgreSQL client detected for social info queue');
    this.verifyDatabaseConnection().catch(error => {
      logger.error('Failed to verify database connection:', error);
    });
  }

  private async verifyDatabaseConnection(): Promise<void> {
    if (!this.pool) return;
    let client: PoolClient | null = null;
    try {
      client = await this.pool.connect();
      const result = await client.query('SELECT COUNT(*) FROM social_info');
      logger.info(`Database connection verified. Current social info count: ${result.rows[0].count}`);
    } catch (error) {
      logger.error('Database connection verification failed:', error);
    } finally {
      if (client) client.release();
    }
  }

  public addToQueue(social: SocialInfo) {
    if (!social.contract_address) {
      logger.warn('Attempted to add social info with missing contract address to queue. Skipping.');
      return;
    }
    this.queue.push(social);
    if (this.queue.length >= this.FLUSH_THRESHOLD && !this.isProcessing) {
      this.flushQueue();
    }
  }

  public addManyToQueue(socials: SocialInfo[]) {
    if (socials.length === 0) return;
    const validSocials = socials.filter(social => {
      if (!social.contract_address) {
        logger.warn('Filtered out social info with missing contract address');
        return false;
      }
      return true;
    });
    this.queue.push(...validSocials);
    logger.info(`Added ${validSocials.length} social infos to queue (total size: ${this.queue.length})`);
    if (this.queue.length >= this.FLUSH_THRESHOLD && !this.isProcessing) {
      this.flushQueue();
    }
  }

  private async persistSocials() {
    if (this.queue.length === 0) return;
    try {
      const dataToSave = { queue: this.queue, timestamp: new Date().toISOString() };
      await fs.promises.writeFile(this.PERSISTENCE_FILE, JSON.stringify(dataToSave), 'utf8');
      logger.info(`Persisted ${this.queue.length} social infos to disk`);
    } catch (error) {
      logger.error('Failed to persist social info queue:', { error, service: "Cherry-Social" });
    }
  }

  private async loadPersistedSocials() {
    try {
      if (fs.existsSync(this.PERSISTENCE_FILE)) {
        const data = await fs.promises.readFile(this.PERSISTENCE_FILE, 'utf8');
        const parsed = JSON.parse(data);
        if (parsed.queue && Array.isArray(parsed.queue)) {
          this.queue = parsed.queue;
          logger.info(`Loaded ${this.queue.length} persisted social infos from disk`);
          if (this.queue.length > 0) {
            logger.info('Scheduling processing of persisted social infos');
            setTimeout(() => this.flushQueue(), 2000);
          }
        }
      }
    } catch (error) {
      logger.error('Failed to load persisted social infos:', { error, service: "Cherry-Social" });
    }
  }

  public async forceFlush(): Promise<boolean> {
    if (!this.pool || this.isProcessing) {
      logger.error('Cannot force flush: Invalid state');
      return false;
    }
    await this.flushQueue();
    return true;
  }

  private async flushQueue(): Promise<void> {
    if (this.queue.length === 0 || this.isProcessing || !this.pool) return;
    let client: PoolClient | null = null;
    let successful = 0;
    let failed = 0;
    const failedSocials: SocialInfo[] = [];

    try {
      this.isProcessing = true;
      logger.info(`Starting queue processing with ${this.queue.length} social infos`);
      client = await this.pool.connect();
      const socialsToProcess = [...this.queue];
      this.queue = [];

      const addresses = socialsToProcess.map(s => s.contract_address);
      const { rows: existingSocials } = await client.query(
        `SELECT contract_address FROM social_info WHERE contract_address = ANY($1)`,
        [addresses]
      );

      const existingAddresses = new Set(existingSocials.map(item => item.contract_address));
      const newSocials = socialsToProcess.filter(s => !existingAddresses.has(s.contract_address));

      if (newSocials.length === 0) {
        logger.info('No new social infos to process');
        return;
      }

      for (const social of newSocials) {
        if (!social.contract_address) {
          failed++;
          failedSocials.push(social);
          logger.warn(`Skipped social info with missing contract_address`);
          continue;
        }

        // Ensure other_social is an array
        const processedSocial = {
          ...social,
          other_social: Array.isArray(social.other_social) ? social.other_social : []
        };

        let socialClient: PoolClient | null = null;
        try {
          socialClient = await this.pool.connect();
          await socialClient.query('BEGIN');

          await socialClient.query(
            `INSERT INTO social_info(
              contract_address, website, telegram, twitter, github, explorer, other_social
            ) VALUES($1, $2, $3, $4, $5, $6, $7)`,
            [
              processedSocial.contract_address,
              processedSocial.website || null,
              processedSocial.telegram || null,
              processedSocial.twitter || null,
              processedSocial.github || null,
              processedSocial.explorer || null,
              processedSocial.other_social
            ]
          );

          await socialClient.query('COMMIT');
          successful++;
          logger.info(`Successfully inserted social info for ${processedSocial.contract_address}`);
        } catch (socialError) {
          logger.error(`Failed to insert social info for ${processedSocial.contract_address}:`, socialError);
          if (socialClient) await socialClient.query('ROLLBACK');
          failed++;
          failedSocials.push(social);
        } finally {
          if (socialClient) socialClient.release();
        }
      }

      if (successful > 0) {
        await client.query('REFRESH MATERIALIZED VIEW CONCURRENTLY social_info_search_view');
        logger.info('Refreshed social_info_search_view materialized view');
      }

      logger.info(`Processed ${successful} social infos, ${failed} failed`);
    } catch (error) {
      logger.error('Error flushing queue:', error);
      if (client) await client.query('ROLLBACK');
     // failedSocials.push(...newSocials);
    } finally {
      if (client) client.release();
      this.isProcessing = false;
      if (failedSocials.length > 0) {
        this.queue.push(...failedSocials);
        logger.info(`Requeued ${failedSocials.length} failed social infos`);
      }
      if (this.queue.length > 0) setTimeout(() => this.flushQueue(), 1000);
    }
  }

  public shutdown() {
    clearInterval(this.flushInterval);
    logger.info('Shutting down social info queue');
    this.persistSocials();
    if (this.queue.length > 0 && this.pool) {
      return this.flushQueue();
    }
    return Promise.resolve();
  }

  public getStats() {
    return {
      queueSize: this.queue.length,
      isProcessing: this.isProcessing
    };
  }
}

function setupShutdownHandlers(queue: SocialInfoQueue, refreshInterval: NodeJS.Timeout): void {
  process.on('SIGINT', async () => {
    clearInterval(refreshInterval);
    await queue.shutdown();
    process.exit(0);
  });
  process.on('SIGTERM', async () => {
    clearInterval(refreshInterval);
    await queue.shutdown();
    process.exit(0);
  });
}

export class SocialInfoModel {
  private pool: Pool;
  private queue: SocialInfoQueue;
  private refreshInterval: NodeJS.Timeout;

  constructor(pool: Pool) {
    this.pool = pool;
    this.queue = SocialInfoQueue.getInstance();
    this.queue.setPool(pool);
    this.refreshInterval = startSocialInfoMaterializedViewRefresh(pool);
    this.testDatabaseAccess().catch(error => {
      logger.error('Initial database access test failed:', error);
    });
  }

  private async testDatabaseAccess(): Promise<void> {
    try {
      const { rows } = await this.pool.query('SELECT COUNT(*) FROM social_info');
      logger.info(`Database initialized with ${rows[0].count} existing social infos`);
    } catch (error) {
      logger.error('Database access test failed:', error);
    }
  }

  async createSocialInfo(socialData: SocialInfo): Promise<void> {
    if (!socialData.contract_address) return;
    this.queue.addToQueue(socialData);
  }

  async bulkCreateSocialInfos(socials: SocialInfo[]): Promise<void> {
    this.queue.addManyToQueue(socials);
  }

  async forceProcessQueue(): Promise<boolean> {
    return this.queue.forceFlush();
  }

  async findByAddress(address: string): Promise<SocialInfo | null> {
    if (!address) {
      logger.warn('Cannot find social info with empty address');
      return null;
    }

    try {
      logger.info(`Looking up social info with address: ${address}`);
      const query = {
        text: `
          SELECT * FROM social_info_search_view WHERE contract_address = $1 LIMIT 1
        `,
        values: [address]
      };
      const result = await this.pool.query(query);
      if (result.rows.length === 0) {
        logger.info(`No social info found for address: ${address}`);
        return null;
      }
      const social = result.rows[0];
      return {
        contract_address: social.contract_address,
        website: social.website,
        telegram: social.telegram,
        twitter: social.twitter,
        github: social.github,
        explorer: social.explorer,
        other_social: social.other_social,
        created_at: social.created_at.toISOString(),
        updated_at: social.updated_at.toISOString()
      };
    } catch (error) {
      logger.error(`Error finding social info with address ${address}:`, error);
      throw error;
    }
  }

  async findByPage(page: number = 1, limit: number = 20): Promise<{ socials: SocialInfo[], total: number }> {
    try {
      const offset = (page - 1) * limit;
      const query = {
        text: `
          SELECT * FROM social_info_search_view
          ORDER BY created_at DESC
          LIMIT $1 OFFSET $2
        `,
        values: [limit, offset]
      };
      const countQuery = { text: `SELECT COUNT(*) as total FROM social_info_search_view` };
      const [result, countResult] = await Promise.all([
        this.pool.query(query),
        this.pool.query(countQuery)
      ]);
      const socials = result.rows.map((social: any) => ({
        contract_address: social.contract_address,
        website: social.website,
        telegram: social.telegram,
        twitter: social.twitter,
        github: social.github,
        explorer: social.explorer,
        other_social: social.other_social,
        created_at: social.created_at.toISOString(),
        updated_at: social.updated_at.toISOString()
      }));
      const total = parseInt(countResult.rows[0].total);
      return { socials, total };
    } catch (error) {
      logger.error('Error fetching social infos by page:', error);
      throw error;
    }
  }

  async updateSocialInfo(address: string, socialData: Partial<SocialInfo>): Promise<boolean> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query(
        `SELECT id FROM social_info WHERE contract_address = $1`,
        [address]
      );
      if (rows.length === 0) {
        await client.query('ROLLBACK');
        return false;
      }
      const updateFields = [];
      const updateValues = [];
      let valueCounter = 1;

      if (socialData.website !== undefined) {
        updateFields.push(`website = $${valueCounter}`);
        updateValues.push(socialData.website || null);
        valueCounter++;
      }
      if (socialData.telegram !== undefined) {
        updateFields.push(`telegram = $${valueCounter}`);
        updateValues.push(socialData.telegram || null);
        valueCounter++;
      }
      if (socialData.twitter !== undefined) {
        updateFields.push(`twitter = $${valueCounter}`);
        updateValues.push(socialData.twitter || null);
        valueCounter++;
      }
      if (socialData.github !== undefined) {
        updateFields.push(`github = $${valueCounter}`);
        updateValues.push(socialData.github || null);
        valueCounter++;
      }
      if (socialData.explorer !== undefined) {
        updateFields.push(`explorer = $${valueCounter}`);
        updateValues.push(socialData.explorer || null);
        valueCounter++;
      }
      if (socialData.other_social !== undefined) {
        updateFields.push(`other_social = $${valueCounter}`);
        updateValues.push(socialData.other_social || []);
        valueCounter++;
      }

      if (updateFields.length > 0) {
        updateFields.push(`updated_at = NOW()`);
        await client.query(
          `UPDATE social_info SET ${updateFields.join(', ')} WHERE contract_address = $${valueCounter}`,
          [...updateValues, address]
        );
      }

      await client.query('COMMIT');
      await client.query('REFRESH MATERIALIZED VIEW CONCURRENTLY social_info_search_view');
      return true;
    } catch (error) {
      logger.error(`Error updating social info ${address}:`, error);
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async deleteSocialInfo(address: string): Promise<boolean> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query(
        `SELECT id FROM social_info WHERE contract_address = $1`,
        [address]
      );
      if (rows.length === 0) {
        await client.query('ROLLBACK');
        return false;
      }
      await client.query(
        `DELETE FROM social_info WHERE contract_address = $1`,
        [address]
      );
      await client.query('COMMIT');
      await client.query('REFRESH MATERIALIZED VIEW CONCURRENTLY social_info_search_view');
      return true;
    } catch (error) {
      logger.error(`Error deleting social info ${address}:`, error);
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async searchSocialInfos(searchText: string, limit: number = 20, offset: number = 0): Promise<{ socials: SocialInfo[], total: number }> {
    try {
      const cacheKey = `search_social_infos:${searchText}:${limit}:${offset}`;
      const cached = await getCache(cacheKey);
      if (cached) {
        logger.info('Returning cached social info search results');
        return cached;
      }

      const tsQuery = searchText
        .trim()
        .split(/\s+/)
        .filter(word => word.length > 0)
        .map(word => `${word}:*`)
        .join(' & ');

      if (!tsQuery) return { socials: [], total: 0 };

      const query = {
        text: `
          SELECT id, contract_address, website, telegram, twitter, github, explorer, other_social, 
                 created_at, updated_at,
                 ts_rank(tsv_search, to_tsquery('english', $1)) as rank
          FROM social_info_search_view
          WHERE tsv_search @@ to_tsquery('english', $1)
          ORDER BY rank DESC
          LIMIT $2 OFFSET $3
        `,
        values: [tsQuery, limit, offset]
      };

      const countQuery = {
        text: `
          SELECT COUNT(*) as total
          FROM social_info_search_view
          WHERE tsv_search @@ to_tsquery('english', $1)
        `,
        values: [tsQuery]
      };

      const [results, countResult] = await Promise.all([
        this.pool.query(query),
        this.pool.query(countQuery)
      ]);

      const socials = results.rows.map((social: any) => ({
        contract_address: social.contract_address,
        website: social.website,
        telegram: social.telegram,
        twitter: social.twitter,
        github: social.github,
        explorer: social.explorer,
        other_social: social.other_social,
        created_at: social.created_at.toISOString(),
        updated_at: social.updated_at.toISOString()
      }));

      const total = parseInt(countResult.rows[0].total);
      const result = { socials, total };

      await setCache(cacheKey, result);
      return result;
    } catch (error) {
      logger.error('Error searching social infos:', error);
      throw error;
    }
  }

  getQueueStats() {
    return this.queue.getStats();
  }

  static async initializeQueue(pool: Pool): Promise<void> {
    logger.info('Initializing social info queue with PostgreSQL client');
    try {
      const testClient = await pool.connect();
      logger.info('Database connection test successful');
      testClient.release();
    } catch (error) {
      logger.error('Database connection test failed:', error);
      throw new Error('Failed to connect to database during initialization');
    }
    await setupSocialDbSchema(pool);
    const queue = SocialInfoQueue.getInstance();
    queue.setPool(pool);
    const refreshInterval = startSocialInfoMaterializedViewRefresh(pool);
    setupShutdownHandlers(queue, refreshInterval);
  }
}