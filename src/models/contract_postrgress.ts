import { Pool, PoolClient } from 'pg';
import { ContractInfo } from '../types/database';
import { logger } from '../config/logger';
import * as fs from 'fs';
import * as path from 'path';
import { setCache, getCache } from '../config/redis';

const setupContractDbSchema = async (pool: Pool): Promise<void> => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    logger.info('Starting PostgreSQL schema setup for contracts');

    // Install PostgreSQL extensions
    const extQuery = `
      CREATE EXTENSION IF NOT EXISTS pg_trgm;
      CREATE EXTENSION IF NOT EXISTS btree_gin;
    `;
    logger.debug('Executing query: Extensions setup\n' + extQuery);
    await client.query(extQuery);
    logger.info('Extensions pg_trgm and btree_gin installed');

    // Create contracts table
    const contractsQuery = `
      CREATE TABLE IF NOT EXISTS contracts (
        id SERIAL PRIMARY KEY,
        contract_address VARCHAR(100) UNIQUE NOT NULL,
        chain VARCHAR(50) NOT NULL,
        symbol VARCHAR(50) NOT NULL,
        token_name VARCHAR(100) NOT NULL,
        token_id INTEGER NOT NULL,
        total_supply BIGINT NOT NULL,
        sol_balance BIGINT NOT NULL,
        usd_balance BIGINT NOT NULL,
        owner VARCHAR(100) NOT NULL,
        decimals INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        tsv_search TSVECTOR GENERATED ALWAYS AS (
          setweight(to_tsvector('english', COALESCE(contract_address, '')), 'A') ||
          setweight(to_tsvector('english', COALESCE(symbol, '')), 'B') ||
          setweight(to_tsvector('english', COALESCE(token_name, '')), 'C')
        ) STORED
      )
    `;
    logger.debug('Executing query: Create contracts table\n' + contractsQuery);
    await client.query(contractsQuery);
    logger.info('Contracts table created');

    // Create materialized view for search
    const materializedViewQuery = `
      CREATE MATERIALIZED VIEW IF NOT EXISTS contracts_search_view AS
      SELECT 
        c.id,
        c.contract_address,
        c.chain,
        c.symbol,
        c.token_name,
        c.token_id,
        c.total_supply,
        c.sol_balance,
        c.usd_balance,
        c.owner,
        c.decimals,
        c.created_at,
        c.updated_at,
        c.tsv_search
      FROM contracts c
      WITH DATA;
    `;
    logger.debug('Executing query: Create contracts_search_view\n' + materializedViewQuery);
    await client.query(materializedViewQuery);
    logger.info('Contracts_search_view materialized view created');

    // Create optimized indexes
    const indexesQuery = `
      CREATE UNIQUE INDEX IF NOT EXISTS idx_contracts_search_view_id ON contracts_search_view(id);
      CREATE INDEX IF NOT EXISTS idx_contracts_search_view_tsv ON contracts_search_view USING GIN(tsv_search);
      CREATE INDEX IF NOT EXISTS idx_contracts_search_view_contract ON contracts_search_view(contract_address);
      CREATE INDEX IF NOT EXISTS idx_contracts_contract_address ON contracts(contract_address);
      CREATE INDEX IF NOT EXISTS idx_contracts_symbol ON contracts(symbol);
      CREATE INDEX IF NOT EXISTS idx_contracts_token_name ON contracts(token_name);
      CREATE INDEX IF NOT EXISTS idx_contracts_contract_address_trgm ON contracts USING GIN(contract_address gin_trgm_ops);
      CREATE INDEX IF NOT EXISTS idx_contracts_symbol_trgm ON contracts USING GIN(symbol gin_trgm_ops);
      CREATE INDEX IF NOT EXISTS idx_contracts_token_name_trgm ON contracts USING GIN(token_name gin_trgm_ops);
    `;
    logger.debug('Executing query: Create indexes\n' + indexesQuery);
    await client.query(indexesQuery);
    logger.info('Indexes created');

    // Create function to refresh materialized view
    const refreshFunctionQuery = `
      CREATE OR REPLACE FUNCTION refresh_contracts_search_view()
      RETURNS void AS $$
      BEGIN
        IF EXISTS (
          SELECT 1
          FROM pg_matviews
          WHERE matviewname = 'contracts_search_view'
        ) THEN
          EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY contracts_search_view';
        END IF;
      EXCEPTION
        WHEN others THEN
          RAISE NOTICE 'Error refreshing contracts_search_view: %', SQLERRM;
      END;
      $$ LANGUAGE plpgsql;
    `;
    logger.debug('Executing query: Create refresh function\n' + refreshFunctionQuery);
    await client.query(refreshFunctionQuery);
    logger.info('Refresh function for contracts_search_view created');

    await client.query('COMMIT');
    logger.info('PostgreSQL contract schema and materialized view setup completed successfully');
  } catch (error) {
    await client.query('ROLLBACK');
    logger.error('Error setting up PostgreSQL contract schema:', error);
    throw error;
  } finally {
    client.release();
    logger.info('Database client released');
  }
};

// Start application-level materialized view refresh
const startContractMaterializedViewRefresh = (pool: Pool): NodeJS.Timeout => {
  const interval = setInterval(async () => {
    try {
      await pool.query('REFRESH MATERIALIZED VIEW CONCURRENTLY contracts_search_view');
      logger.info('Contracts materialized view refreshed by application');
    } catch (error) {
      logger.error('Error refreshing contracts materialized view in application:', error);
    }
  }, 5 * 60 * 1000); // Refresh every 5 minutes
  return interval;
};

export class ContractQueue {
  private static instance: ContractQueue;
  private queue: ContractInfo[] = [];
  private isProcessing: boolean = false;
  private flushInterval: NodeJS.Timeout;
  private pool: Pool | null = null;
  private readonly FLUSH_THRESHOLD = 5;
  private readonly PERSISTENCE_FILE = path.join(process.cwd(), 'contract_queue_backup.json');

  private constructor() {
    this.flushInterval = setInterval(() => this.flushQueue(), 3000);
    this.loadPersistedContracts();
    setInterval(() => this.persistContracts(), 30000);
    setInterval(() => {
      const stats = this.getStats();
      logger.info(`Contract queue status: ${stats.queueSize} contracts in queue, processing: ${stats.isProcessing}`);
    }, 10000);
  }

  public static getInstance(): ContractQueue {
    if (!ContractQueue.instance) {
      ContractQueue.instance = new ContractQueue();
    }
    return ContractQueue.instance;
  }

  public setPool(pool: Pool) {
    this.pool = pool;
    logger.info('PostgreSQL client detected for contract queue');
    this.verifyDatabaseConnection().catch(error => {
      logger.error('Failed to verify database connection:', error);
    });
  }

  private async verifyDatabaseConnection(): Promise<void> {
    if (!this.pool) return;
    let client: PoolClient | null = null;
    try {
      client = await this.pool.connect();
      const result = await client.query('SELECT COUNT(*) FROM contracts');
      logger.info(`Database connection verified. Current contract count: ${result.rows[0].count}`);
    } catch (error) {
      logger.error('Database connection verification failed:', error);
    } finally {
      if (client) client.release();
    }
  }

  public addToQueue(contract: ContractInfo) {
    if (!contract.contract_address) {
      logger.warn('Attempted to add contract with missing contract address to queue. Skipping.');
      return;
    }
    this.queue.push(contract);
    if (this.queue.length >= this.FLUSH_THRESHOLD && !this.isProcessing) {
      this.flushQueue();
    }
  }

  public addManyToQueue(contracts: ContractInfo[]) {
    if (contracts.length === 0) return;
    const validContracts = contracts.filter(contract => {
      if (!contract.contract_address) {
        logger.warn('Filtered out contract with missing contract address');
        return false;
      }
      return true;
    });
    this.queue.push(...validContracts);
    logger.info(`Added ${validContracts.length} contracts to queue (total size: ${this.queue.length})`);
    if (this.queue.length >= this.FLUSH_THRESHOLD && !this.isProcessing) {
      this.flushQueue();
    }
  }

  private async persistContracts() {
    if (this.queue.length === 0) return;
    try {
      const dataToSave = { queue: this.queue, timestamp: new Date().toISOString() };
      await fs.promises.writeFile(this.PERSISTENCE_FILE, JSON.stringify(dataToSave), 'utf8');
      logger.info(`Persisted ${this.queue.length} contracts to disk`);
    } catch (error) {
      logger.error('Failed to persist contract queue:', { error, service: "Cherry-Contract" });
    }
  }

  private async loadPersistedContracts() {
    try {
      if (fs.existsSync(this.PERSISTENCE_FILE)) {
        const data = await fs.promises.readFile(this.PERSISTENCE_FILE, 'utf8');
        const parsed = JSON.parse(data);
        if (parsed.queue && Array.isArray(parsed.queue)) {
          this.queue = parsed.queue;
          logger.info(`Loaded ${this.queue.length} persisted contracts from disk`);
          if (this.queue.length > 0) {
            logger.info('Scheduling processing of persisted contracts');
            setTimeout(() => this.flushQueue(), 2000);
          }
        }
      }
    } catch (error) {
      logger.error('Failed to load persisted contracts:', { error, service: "Cherry-Contract" });
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
    const failedContracts: ContractInfo[] = [];

    try {
      this.isProcessing = true;
      logger.info(`Starting queue processing with ${this.queue.length} contracts`);
      client = await this.pool.connect();
      const contractsToProcess = [...this.queue];
      this.queue = [];

      const addresses = contractsToProcess.map(c => c.contract_address);
      const { rows: existingContracts } = await client.query(
        `SELECT contract_address FROM contracts WHERE contract_address = ANY($1)`,
        [addresses]
      );

      const existingAddresses = new Set(existingContracts.map(item => item.contract_address));
      const newContracts = contractsToProcess.filter(c => !existingAddresses.has(c.contract_address));

      if (newContracts.length === 0) {
        logger.info('No new contracts to process');
        return;
      }

      for (const contract of newContracts) {
        if (!contract.contract_address) {
          failed++;
          failedContracts.push(contract);
          logger.warn(`Skipped contract with missing contract_address`);
          continue;
        }

        // Preprocess balances to ensure they are integers
        const processedContract = {
          ...contract,
          sol_balance: Math.floor(Number(contract.sol_balance) || 0),
          usd_balance: Math.floor(Number(contract.usd_balance) || 0),
          total_supply: Math.floor(Number(contract.total_supply) || 0)
        };

        // Use a separate transaction for each contract
        let contractClient: PoolClient | null = null;
        try {
          contractClient = await this.pool.connect();
          await contractClient.query('BEGIN');

          await contractClient.query(
            `INSERT INTO contracts(
              contract_address, chain, symbol, token_name, token_id, total_supply, 
              sol_balance, usd_balance, owner, decimals
            ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
            [
              processedContract.contract_address,
              processedContract.chain,
              processedContract.symbol,
              processedContract.token_name,
              processedContract.token_id,
              processedContract.total_supply,
              processedContract.sol_balance,
              processedContract.usd_balance,
              processedContract.owner,
              processedContract.decimals
            ]
          );

          await contractClient.query('COMMIT');
          successful++;
          logger.info(`Successfully inserted contract ${processedContract.contract_address}`);
        } catch (contractError) {
          logger.error(`Failed to insert contract ${processedContract.contract_address}:`, contractError);
          if (contractClient) await contractClient.query('ROLLBACK');
          failed++;
          failedContracts.push(contract);
        } finally {
          if (contractClient) contractClient.release();
        }
      }

      // Refresh materialized view only if there were successful insertions
      if (successful > 0) {
        await client.query('REFRESH MATERIALIZED VIEW CONCURRENTLY contracts_search_view');
        logger.info('Refreshed contracts_search_view materialized view');
      }

      logger.info(`Processed ${successful} contracts, ${failed} failed`);
    } catch (error) {
      logger.error('Error flushing queue:', error);
      if (client) await client.query('ROLLBACK');
      const newContracts: ContractInfo[] = []; // Ensure newContracts is defined
      failedContracts.push(...newContracts);
    } finally {
      if (client) client.release();
      this.isProcessing = false;
      // Requeue failed contracts
      if (failedContracts.length > 0) {
        this.queue.push(...failedContracts);
        logger.info(`Requeued ${failedContracts.length} failed contracts`);
      }
      if (this.queue.length > 0) setTimeout(() => this.flushQueue(), 1000);
    }
  }

  public shutdown() {
    clearInterval(this.flushInterval);
    logger.info('Shutting down contract queue');
    this.persistContracts();
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

function setupShutdownHandlers(queue: ContractQueue, refreshInterval: NodeJS.Timeout): void {
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

export class ContractModel {
  private pool: Pool;
  private queue: ContractQueue;
  private refreshInterval: NodeJS.Timeout;

  constructor(pool: Pool) {
    this.pool = pool;
    this.queue = ContractQueue.getInstance();
    this.queue.setPool(pool);
    this.refreshInterval = startContractMaterializedViewRefresh(pool);
    this.testDatabaseAccess().catch(error => {
      logger.error('Initial database access test failed:', error);
    });
  }

  private async testDatabaseAccess(): Promise<void> {
    try {
      const { rows } = await this.pool.query('SELECT COUNT(*) FROM contracts');
      logger.info(`Database initialized with ${rows[0].count} existing contracts`);
    } catch (error) {
      logger.error('Database access test failed:', error);
    }
  }

  async createContract(contractData: ContractInfo): Promise<void> {
    if (!contractData.contract_address) return;
    this.queue.addToQueue(contractData);
  }

  async bulkCreateContracts(contracts: ContractInfo[]): Promise<void> {
    this.queue.addManyToQueue(contracts);
  }

  async forceProcessQueue(): Promise<boolean> {
    return this.queue.forceFlush();
  }

  async findByAddress(address: string): Promise<ContractInfo | null> {
    if (!address) {
      logger.warn('Cannot find contract with empty address');
      return null;
    }

    try {
      logger.info(`Looking up contract with address: ${address}`);
      const query = {
        text: `
          SELECT * FROM contracts_search_view WHERE contract_address = $1 LIMIT 1
        `,
        values: [address]
      };
      const result = await this.pool.query(query);
      if (result.rows.length === 0) {
        logger.info(`No contract found for address: ${address}`);
        return null;
      }
      const contract = result.rows[0];
      return {
        contract_address: contract.contract_address,
        chain: contract.chain,
        symbol: contract.symbol,
        token_name: contract.token_name,
        token_id: contract.token_id,
        total_supply: contract.total_supply,
        sol_balance: contract.sol_balance,
        usd_balance: contract.usd_balance,
        owner: contract.owner,
        decimals: contract.decimals,
        created_at: contract.created_at.toISOString(),
        updated_at: contract.updated_at.toISOString()
      };
    } catch (error) {
      logger.error(`Error finding contract with address ${address}:`, error);
      throw error;
    }
  }

  async findContractsByPage(page: number = 1, limit: number = 20): Promise<{ contracts: ContractInfo[], total: number }> {
    try {
      const offset = (page - 1) * limit;
      const query = {
        text: `
          SELECT * FROM contracts_search_view
          ORDER BY created_at DESC
          LIMIT $1 OFFSET $2
        `,
        values: [limit, offset]
      };
      const countQuery = { text: `SELECT COUNT(*) as total FROM contracts_search_view` };
      const [result, countResult] = await Promise.all([
        this.pool.query(query),
        this.pool.query(countQuery)
      ]);
      const contracts = result.rows.map((contract: any) => ({
        contract_address: contract.contract_address,
        chain: contract.chain,
        symbol: contract.symbol,
        token_name: contract.token_name,
        token_id: contract.token_id,
        total_supply: contract.total_supply,
        sol_balance: contract.sol_balance,
        usd_balance: contract.usd_balance,
        owner: contract.owner,
        decimals: contract.decimals,
        created_at: contract.created_at.toISOString(),
        updated_at: contract.updated_at.toISOString()
      }));
      const total = parseInt(countResult.rows[0].total);
      return { contracts, total };
    } catch (error) {
      logger.error('Error fetching contracts by page:', error);
      throw error;
    }
  }

  async updateContractInfo(address: string, contractData: Partial<ContractInfo>): Promise<boolean> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query(
        `SELECT id FROM contracts WHERE contract_address = $1`,
        [address]
      );
      if (rows.length === 0) {
        await client.query('ROLLBACK');
        return false;
      }
      const updateFields = [];
      const updateValues = [];
      let valueCounter = 1;

      if (contractData.chain !== undefined) {
        updateFields.push(`chain = $${valueCounter}`);
        updateValues.push(contractData.chain);
        valueCounter++;
      }
      if (contractData.symbol !== undefined) {
        updateFields.push(`symbol = $${valueCounter}`);
        updateValues.push(contractData.symbol);
        valueCounter++;
      }
      if (contractData.token_name !== undefined) {
        updateFields.push(`token_name = $${valueCounter}`);
        updateValues.push(contractData.token_name);
        valueCounter++;
      }
      if (contractData.token_id !== undefined) {
        updateFields.push(`token_id = $${valueCounter}`);
        updateValues.push(contractData.token_id);
        valueCounter++;
      }
      if (contractData.total_supply !== undefined) {
        updateFields.push(`total_supply = $${valueCounter}`);
        updateValues.push(Math.floor(Number(contractData.total_supply) || 0));
        valueCounter++;
      }
      if (contractData.sol_balance !== undefined) {
        updateFields.push(`sol_balance = $${valueCounter}`);
        updateValues.push(Math.floor(Number(contractData.sol_balance) || 0));
        valueCounter++;
      }
      if (contractData.usd_balance !== undefined) {
        updateFields.push(`usd_balance = $${valueCounter}`);
        updateValues.push(Math.floor(Number(contractData.usd_balance) || 0));
        valueCounter++;
      }
      if (contractData.owner !== undefined) {
        updateFields.push(`owner = $${valueCounter}`);
        updateValues.push(contractData.owner);
        valueCounter++;
      }
      if (contractData.decimals !== undefined) {
        updateFields.push(`decimals = $${valueCounter}`);
        updateValues.push(contractData.decimals);
        valueCounter++;
      }

      if (updateFields.length > 0) {
        updateFields.push(`updated_at = NOW()`);
        await client.query(
          `UPDATE contracts SET ${updateFields.join(', ')} WHERE contract_address = $${valueCounter}`,
          [...updateValues, address]
        );
      }

      await client.query('COMMIT');
      await client.query('REFRESH MATERIALIZED VIEW CONCURRENTLY contracts_search_view');
      return true;
    } catch (error) {
      logger.error(`Error updating contract ${address}:`, error);
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async searchContracts(searchText: string, limit: number = 20, offset: number = 0): Promise<{ contracts: ContractInfo[], total: number }> {
    try {
      const cacheKey = `search_contracts:${searchText}:${limit}:${offset}`;
      const cached = await getCache(cacheKey);
      if (cached) {
        logger.info('Returning cached contract search results');
        return cached;
      }

      const tsQuery = searchText
        .trim()
        .split(/\s+/)
        .filter(word => word.length > 0)
        .map(word => `${word}:*`)
        .join(' & ');

      if (!tsQuery) return { contracts: [], total: 0 };

      const query = {
        text: `
          SELECT id, contract_address, chain, symbol, token_name, token_id, total_supply, 
                 sol_balance, usd_balance, owner, decimals, created_at, updated_at,
                 ts_rank(tsv_search, to_tsquery('english', $1)) as rank
          FROM contracts_search_view
          WHERE tsv_search @@ to_tsquery('english', $1)
          ORDER BY rank DESC
          LIMIT $2 OFFSET $3
        `,
        values: [tsQuery, limit, offset]
      };

      const countQuery = {
        text: `
          SELECT COUNT(*) as total
          FROM contracts_search_view
          WHERE tsv_search @@ to_tsquery('english', $1)
        `,
        values: [tsQuery]
      };

      const [results, countResult] = await Promise.all([
        this.pool.query(query),
        this.pool.query(countQuery)
      ]);

      const contracts = results.rows.map((contract: any) => ({
        contract_address: contract.contract_address,
        chain: contract.chain,
        symbol: contract.symbol,
        token_name: contract.token_name,
        token_id: contract.token_id,
        total_supply: contract.total_supply,
        sol_balance: contract.sol_balance,
        usd_balance: contract.usd_balance,
        owner: contract.owner,
        decimals: contract.decimals,
        created_at: contract.created_at.toISOString(),
        updated_at: contract.updated_at.toISOString()
      }));

      const total = parseInt(countResult.rows[0].total);
      const result = { contracts, total };

      await setCache(cacheKey, result);
      return result;
    } catch (error) {
      logger.error('Error searching contracts:', error);
      throw error;
    }
  }

  getQueueStats() {
    return this.queue.getStats();
  }

  static async initializeQueue(pool: Pool): Promise<void> {
    logger.info('Initializing contract queue with PostgreSQL client');
    try {
      const testClient = await pool.connect();
      logger.info('Database connection test successful');
      testClient.release();
    } catch (error) {
      logger.error('Database connection test failed:', error);
      throw new Error('Failed to connect to database during initialization');
    }
    await setupContractDbSchema(pool);
    const queue = ContractQueue.getInstance();
    queue.setPool(pool);
    const refreshInterval = startContractMaterializedViewRefresh(pool);
    setupShutdownHandlers(queue, refreshInterval);
  }
}