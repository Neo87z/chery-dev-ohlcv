import { ClickHouseClient } from '@clickhouse/client';
import { TokenInfo } from '../types/database';
import { logger } from '../config/logger';
import * as fs from 'fs';
import * as path from 'path';

export class TokenInfoQueue {
  private static instance: TokenInfoQueue;
  private queue: TokenInfo[] = [];
  private isProcessing: boolean = false;
  private flushInterval: NodeJS.Timeout;
  private client: ClickHouseClient | null = null;
  private readonly FLUSH_THRESHOLD = 10;
  private readonly PERSISTENCE_FILE = path.join(process.cwd(), 'token_info_queue_backup.json');
  
  private constructor() {
    this.flushInterval = setInterval(() => this.flushQueue(), 5000);
    this.loadPersistedTokens();
    
    setInterval(() => this.persistTokens(), 60000);
  }

  public static getInstance(): TokenInfoQueue {
    if (!TokenInfoQueue.instance) {
      TokenInfoQueue.instance = new TokenInfoQueue();
    }
    return TokenInfoQueue.instance;
  }

  public setClient(client: ClickHouseClient) {
    this.client = client;
  }

  public addToQueue(token: TokenInfo) {
    this.queue.push(token);
    
    if (this.queue.length >= this.FLUSH_THRESHOLD && !this.isProcessing) {
      this.flushQueue();
    }
  }

  public addManyToQueue(tokens: TokenInfo[]) {
    if (tokens.length === 0) return;
    
    this.queue.push(...tokens);
    logger.info(`Added ${tokens.length} tokens to queue (total size: ${this.queue.length})`);
    
    if (this.queue.length >= this.FLUSH_THRESHOLD && !this.isProcessing) {
      this.flushQueue();
    }
  }

  private async persistTokens() {
    if (this.queue.length === 0) {
      return;
    }

    try {
      const dataToSave = {
        queue: this.queue,
        timestamp: new Date().toISOString()
      };

      await fs.promises.writeFile(
        this.PERSISTENCE_FILE,
        JSON.stringify(dataToSave),
        'utf8'
      );
      
      logger.info(`Persisted ${this.queue.length} tokens to disk`);
    } catch (error) {
      logger.error('Failed to persist token queue:', { error, service: "Cherry-OHLCV" });
    }
  }

  private async loadPersistedTokens() {
    try {
      if (fs.existsSync(this.PERSISTENCE_FILE)) {
        const data = await fs.promises.readFile(this.PERSISTENCE_FILE, 'utf8');
        const parsed = JSON.parse(data);
        
        if (parsed.queue && Array.isArray(parsed.queue)) {
          this.queue = parsed.queue;
        }
        
        logger.info(`Loaded ${this.queue.length} persisted tokens from disk`);
      }
    } catch (error) {
      logger.error('Failed to load persisted tokens:', { error, service: "Cherry-OHLCV" });
    }
  }

  private async flushQueue() {
    if (this.queue.length === 0 || this.isProcessing || !this.client) {
      return;
    }

    try {
      this.isProcessing = true;

      const tokensToProcess = [...this.queue];
      this.queue = [];

      logger.info(`Processing ${tokensToProcess.length} tokens`);

      const addresses = tokensToProcess.map(t => `'${t.contract_address}'`).join(',');
      const query = `
        SELECT contract_address 
        FROM token_info
        WHERE contract_address IN (${addresses})
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const existingAddresses = new Set((await result.json<{contract_address: string}[]>())
        .map(item => item.contract_address));
      
      const newTokens = tokensToProcess.filter(t => !existingAddresses.has(t.contract_address));
      
      if (newTokens.length === 0) {
        logger.info('All tokens already exist, nothing to insert');
        return;
      }

      // Process tokens for insertion
      const tokensToInsert = newTokens.map(token => ({
        ...token,
        // Ensure makers is properly formatted as an array for ClickHouse
        makers: Array.isArray(token.makers) ? token.makers : (token.makers ? [token.makers] : [])
      }));

      await this.client.insert({
        table: 'token_info',
        values: tokensToInsert,
        format: 'JSONEachRow'
      });
      
      logger.info(`Successfully inserted ${tokensToInsert.length} tokens`);
      
    } catch (error) {
      logger.error('Error flushing token queue:', { error, service: "Cherry-OHLCV" });
      
      // Put the failed tokens back in the queue
      this.queue = [...this.queue, ...this.queue];
      
    } finally {
      this.isProcessing = false;
    }
  }

  public shutdown() {
    clearInterval(this.flushInterval);
    
    this.persistTokens();
    
    if (this.queue.length > 0 && this.client) {
      this.flushQueue();
    }
    
    logger.info(`Token queue shut down. ${this.queue.length} tokens remaining in queue and persisted to disk.`);
  }

  public getStats() {
    return {
      queueSize: this.queue.length,
      isProcessing: this.isProcessing
    };
  }
}

export class TokenInfoModel {
  createToken(tokenInfo: TokenInfo) {
    throw new Error('Method not implemented.');
  }
  private client: ClickHouseClient;
  private queue: TokenInfoQueue;
  bulkCreateTokens: any;

  constructor(client: ClickHouseClient) {
    this.client = client;
    this.queue = TokenInfoQueue.getInstance();
    this.queue.setClient(client);
  }

  async findByAddress(address: string): Promise<TokenInfo | null> {
    try {
      const query = `
        SELECT * FROM token_info
        WHERE contract_address = '${address}'
        LIMIT 1
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const data = await result.json<TokenInfo[]>();
      return data.length > 0 ? data[0] : null;
    } catch (error) {
      logger.error('Error in findByAddress:', error);
      throw error;
    }
  }

  async createTokenInfo(tokenData: TokenInfo): Promise<void> {
    this.queue.addToQueue(tokenData);
  }

  async bulkCreateTokenInfos(tokens: TokenInfo[]): Promise<void> {
    this.queue.addManyToQueue(tokens);
  }

  getQueueStats() {
    return this.queue.getStats();
  }

  static initializeQueue(client: ClickHouseClient): void {
    const queue = TokenInfoQueue.getInstance();
    queue.setClient(client);
    logger.info('Token info queue initialized');
    
    process.on('SIGINT', () => {
      queue.shutdown();
    });
    
    process.on('SIGTERM', () => {
      queue.shutdown();
    });
  }

  async findTokensByPage(page: number = 1, limit: number = 20): Promise<{tokens: TokenInfo[], total: number}> {
    try {
      const offset = (page - 1) * limit;
      
      const query = `
        SELECT * FROM token_info
        ORDER BY create_date DESC
        LIMIT ${limit} OFFSET ${offset}
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const tokens = await result.json<TokenInfo[]>();
      
      // Get total count
      const countQuery = `
        SELECT count() as total FROM token_info
      `;
      
      const countResult = await this.client.query({
        query: countQuery,
        format: 'JSONEachRow'
      });
      
      const countData = await countResult.json<[{ total: number }]>();
      const total = countData[0]?.total || 0;
      
      return { tokens, total };
    } catch (error) {
      logger.error('Error fetching tokens by page:', error);
      throw error;
    }
  }

  async updateTokenInfo(address: string, tokenData: Partial<TokenInfo>): Promise<boolean> {
    try {
      // Check if the token exists
      const existingToken = await this.findByAddress(address);
      
      if (!existingToken) {
        return false;
      }

      const updateFields = [];

      if (tokenData.create_date) updateFields.push(`create_date = '${tokenData.create_date}'`);
      if (tokenData.volume) updateFields.push(`volume = '${tokenData.volume}'`);
      if (tokenData.image_link) updateFields.push(`image_link = '${tokenData.image_link}'`);
      if (tokenData.holders !== undefined) updateFields.push(`holders = ${tokenData.holders}`);
      
      // Handle array field separately
      if (tokenData.makers && Array.isArray(tokenData.makers)) {
        const makersArray = tokenData.makers.map(maker => `'${maker}'`).join(', ');
        updateFields.push(`makers = [${makersArray}]`);
      }

      if (updateFields.length === 0) {
        return false;
      }

      const updateQuery = `
        ALTER TABLE token_info
        UPDATE ${updateFields.join(', ')}
        WHERE contract_address = '${address}'
      `;

      await this.client.query({
        query: updateQuery
      });

      return true;
    } catch (error) {
      logger.error('Error updating token info:', error);
      throw error;
    }
  }

  async deleteTokenInfo(address: string): Promise<boolean> {
    try {
      // Check if the token exists
      const existingToken = await this.findByAddress(address);
      
      if (!existingToken) {
        return false;
      }

      const deleteQuery = `
        ALTER TABLE token_info
        DELETE WHERE contract_address = '${address}'
      `;

      await this.client.query({
        query: deleteQuery
      });

      return true;
    } catch (error) {
      logger.error('Error deleting token info:', error);
      throw error;
    }
  }
}