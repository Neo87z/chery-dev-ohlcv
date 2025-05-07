import { ClickHouseClient } from '@clickhouse/client';
import { TokenStatistics } from '../types/database';
import { logger } from '../config/logger';
import * as fs from 'fs';
import * as path from 'path';

export class TokenStatisticsQueue {
  private static instance: TokenStatisticsQueue;
  private queue: TokenStatistics[] = [];
  private isProcessing: boolean = false;
  private flushInterval: NodeJS.Timeout;
  private client: ClickHouseClient | null = null;
  private readonly FLUSH_THRESHOLD = 10;
  private readonly PERSISTENCE_FILE = path.join(process.cwd(), 'token_statistics_queue_backup.json');
  
  private constructor() {
    this.flushInterval = setInterval(() => this.flushQueue(), 5000);
    this.loadPersistedStatistics();
    
    setInterval(() => this.persistStatistics(), 60000);
  }

  public static getInstance(): TokenStatisticsQueue {
    if (!TokenStatisticsQueue.instance) {
      TokenStatisticsQueue.instance = new TokenStatisticsQueue();
    }
    return TokenStatisticsQueue.instance;
  }

  public setClient(client: ClickHouseClient) {
    this.client = client;
  }

  public addToQueue(statistics: TokenStatistics) {
    this.queue.push(statistics);
    
    if (this.queue.length >= this.FLUSH_THRESHOLD && !this.isProcessing) {
      this.flushQueue();
    }
  }

  public addManyToQueue(statistics: TokenStatistics[]) {
    if (statistics.length === 0) return;
    
    this.queue.push(...statistics);
    logger.info(`Added ${statistics.length} token statistics to queue (total size: ${this.queue.length})`);
    
    if (this.queue.length >= this.FLUSH_THRESHOLD && !this.isProcessing) {
      this.flushQueue();
    }
  }

  private async persistStatistics() {
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
      
      logger.info(`Persisted ${this.queue.length} token statistics to disk`);
    } catch (error) {
      logger.error('Failed to persist token statistics queue:', { error, service: "Cherry-OHLCV" });
    }
  }

  private async loadPersistedStatistics() {
    try {
      if (fs.existsSync(this.PERSISTENCE_FILE)) {
        const data = await fs.promises.readFile(this.PERSISTENCE_FILE, 'utf8');
        const parsed = JSON.parse(data);
        
        if (parsed.queue && Array.isArray(parsed.queue)) {
          this.queue = parsed.queue;
        }
        
        logger.info(`Loaded ${this.queue.length} persisted token statistics from disk`);
      }
    } catch (error) {
      logger.error('Failed to load persisted token statistics:', { error, service: "Cherry-OHLCV" });
    }
  }

  private async flushQueue() {
    if (this.queue.length === 0 || this.isProcessing || !this.client) {
      return;
    }

    try {
      this.isProcessing = true;

      const statisticsToProcess = [...this.queue];
      this.queue = [];

      logger.info(`Processing ${statisticsToProcess.length} token statistics`);

      const addresses = statisticsToProcess.map(s => `'${s.contract_address}'`).join(',');
      const query = `
        SELECT contract_address 
        FROM token_statistics
        WHERE contract_address IN (${addresses})
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const existingAddresses = new Set((await result.json<{contract_address: string}[]>())
        .map(item => item.contract_address));
      
      const newStatistics = statisticsToProcess.filter(s => !existingAddresses.has(s.contract_address));
      
      if (newStatistics.length === 0) {
        logger.info('All token statistics already exist, nothing to insert');
        return;
      }

      await this.client.insert({
        table: 'token_statistics',
        values: newStatistics,
        format: 'JSONEachRow'
      });
      
      logger.info(`Successfully inserted ${newStatistics.length} token statistics`);
      
    } catch (error) {
      logger.error('Error flushing token statistics queue:', { error, service: "Cherry-OHLCV" });
      
      // Put the failed statistics back in the queue
      this.queue = [...this.queue, ...this.queue];
      
    } finally {
      this.isProcessing = false;
    }
  }

  public shutdown() {
    clearInterval(this.flushInterval);
    
    this.persistStatistics();
    
    if (this.queue.length > 0 && this.client) {
      this.flushQueue();
    }
    
    logger.info(`Token statistics queue shut down. ${this.queue.length} statistics remaining in queue and persisted to disk.`);
  }

  public getStats() {
    return {
      queueSize: this.queue.length,
      isProcessing: this.isProcessing
    };
  }
}

export class TokenStatisticsModel {
  createStats(tokenStatistics: TokenStatistics) {
    throw new Error('Method not implemented.');
  }
  private client: ClickHouseClient;
  private queue: TokenStatisticsQueue;
  bulkCreateStats: any;

  constructor(client: ClickHouseClient) {
    this.client = client;
    this.queue = TokenStatisticsQueue.getInstance();
    this.queue.setClient(client);
  }

  async findByAddress(address: string): Promise<TokenStatistics | null> {
    try {
      const query = `
        SELECT * FROM token_statistics
        WHERE contract_address = '${address}'
        LIMIT 1
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const data = await result.json<TokenStatistics[]>();
      return data.length > 0 ? data[0] : null;
    } catch (error) {
      logger.error('Error in findByAddress:', error);
      throw error;
    }
  }

  async createTokenStatistics(statisticsData: TokenStatistics): Promise<void> {
    this.queue.addToQueue(statisticsData);
  }

  async bulkCreateTokenStatistics(statistics: TokenStatistics[]): Promise<void> {
    this.queue.addManyToQueue(statistics);
  }

  getQueueStats() {
    return this.queue.getStats();
  }

  static initializeQueue(client: ClickHouseClient): void {
    const queue = TokenStatisticsQueue.getInstance();
    queue.setClient(client);
    logger.info('Token statistics queue initialized');
    
    process.on('SIGINT', () => {
      queue.shutdown();
    });
    
    process.on('SIGTERM', () => {
      queue.shutdown();
    });
  }

  async findStatisticsByPage(page: number = 1, limit: number = 20): Promise<{statistics: TokenStatistics[], total: number}> {
    try {
      const offset = (page - 1) * limit;
      
      const query = `
        SELECT * FROM token_statistics
        ORDER BY marketcap DESC
        LIMIT ${limit} OFFSET ${offset}
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const statistics = await result.json<TokenStatistics[]>();
      
      // Get total count
      const countQuery = `
        SELECT count() as total FROM token_statistics
      `;
      
      const countResult = await this.client.query({
        query: countQuery,
        format: 'JSONEachRow'
      });
      
      const countData = await countResult.json<[{ total: number }]>();
      const total = countData[0]?.total || 0;
      
      return { statistics, total };
    } catch (error) {
      logger.error('Error fetching token statistics by page:', error);
      throw error;
    }
  }

  async updateTokenStatistics(address: string, statisticsData: Partial<TokenStatistics>): Promise<boolean> {
    try {
      // Check if the statistics exist
      const existingStatistics = await this.findByAddress(address);
      
      if (!existingStatistics) {
        return false;
      }

      const updateFields = [];

      if (statisticsData.marketcap) updateFields.push(`marketcap = '${statisticsData.marketcap}'`);
      if (statisticsData.is_trending !== undefined) updateFields.push(`is_trending = ${statisticsData.is_trending}`);
      if (statisticsData.is_tracked !== undefined) updateFields.push(`is_tracked = ${statisticsData.is_tracked}`);
      if (statisticsData.total_supply) updateFields.push(`total_supply = '${statisticsData.total_supply}'`);
      if (statisticsData.current_price) updateFields.push(`current_price = '${statisticsData.current_price}'`);

      if (updateFields.length === 0) {
        return false;
      }

      const updateQuery = `
        ALTER TABLE token_statistics
        UPDATE ${updateFields.join(', ')}
        WHERE contract_address = '${address}'
      `;

      await this.client.query({
        query: updateQuery
      });

      return true;
    } catch (error) {
      logger.error('Error updating token statistics:', error);
      throw error;
    }
  }

  async deleteTokenStatistics(address: string): Promise<boolean> {
    try {
      // Check if the statistics exist
      const existingStatistics = await this.findByAddress(address);
      
      if (!existingStatistics) {
        return false;
      }

      const deleteQuery = `
        ALTER TABLE token_statistics
        DELETE WHERE contract_address = '${address}'
      `;

      await this.client.query({
        query: deleteQuery
      });

      return true;
    } catch (error) {
      logger.error('Error deleting token statistics:', error);
      throw error;
    }
  }

  
  
  // Get tracked tokens
  async findTrackedTokens(page: number = 1, limit: number = 10): Promise<{statistics: TokenStatistics[], total: number}> {
    try {
      const offset = (page - 1) * limit;
      
      const query = `
        SELECT * FROM token_statistics
        WHERE is_tracked = 1
        ORDER BY CAST(marketcap AS Float64) DESC
        LIMIT ${limit} OFFSET ${offset}
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const statistics = await result.json<TokenStatistics[]>();
      
      // Get total count of tracked tokens
      const countQuery = `
        SELECT count() as total FROM token_statistics
        WHERE is_tracked = 1
      `;
      
      const countResult = await this.client.query({
        query: countQuery,
        format: 'JSONEachRow'
      });
      
      const countData = await countResult.json<[{ total: number }]>();
      const total = countData[0]?.total || 0;
      
      return { statistics, total };
    } catch (error) {
      logger.error('Error fetching tracked token statistics:', error);
      throw error;
    }
  }
  
  // Get tokens with marketcap above threshold
  async findTokensByMarketcapThreshold(threshold: string, page: number = 1, limit: number = 20): Promise<{statistics: TokenStatistics[], total: number}> {
    try {
      const offset = (page - 1) * limit;
      
      const query = `
        SELECT * FROM token_statistics
        WHERE CAST(marketcap AS Float64) > ${parseFloat(threshold)}
        ORDER BY CAST(marketcap AS Float64) DESC
        LIMIT ${limit} OFFSET ${offset}
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const statistics = await result.json<TokenStatistics[]>();
      
      // Get total count
      const countQuery = `
        SELECT count() as total FROM token_statistics
        WHERE CAST(marketcap AS Float64) > ${parseFloat(threshold)}
      `;
      
      const countResult = await this.client.query({
        query: countQuery,
        format: 'JSONEachRow'
      });
      
      const countData = await countResult.json<[{ total: number }]>();
      const total = countData[0]?.total || 0;
      
      return { statistics, total };
    } catch (error) {
      logger.error('Error fetching token statistics by marketcap threshold:', error);
      throw error;
    }
  }
  
  // Get tokens with highest price
  async findTopPricedTokens(page: number = 1, limit: number = 20): Promise<{statistics: TokenStatistics[], total: number}> {
    try {
      const offset = (page - 1) * limit;
      
      const query = `
        SELECT * FROM token_statistics
        ORDER BY CAST(current_price AS Float64) DESC
        LIMIT ${limit} OFFSET ${offset}
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const statistics = await result.json<TokenStatistics[]>();
      
      // Get total count
      const countQuery = `
        SELECT count() as total FROM token_statistics
      `;
      
      const countResult = await this.client.query({
        query: countQuery,
        format: 'JSONEachRow'
      });
      
      const countData = await countResult.json<[{ total: number }]>();
      const total = countData[0]?.total || 0;
      
      return { statistics, total };
    } catch (error) {
      logger.error('Error fetching top priced token statistics:', error);
      throw error;
    }
  }
  
  // Set token as trending
  async setTokenTrending(address: string, isTrending: boolean): Promise<boolean> {
    try {
      const existingStatistics = await this.findByAddress(address);
      
      if (!existingStatistics) {
        return false;
      }
      
      const updateQuery = `
        ALTER TABLE token_statistics
        UPDATE is_trending = ${isTrending ? 1 : 0}
        WHERE contract_address = '${address}'
      `;
      
      await this.client.query({
        query: updateQuery
      });
      
      return true;
    } catch (error) {
      logger.error('Error setting token trending status:', error);
      throw error;
    }
  }
  
  // Set token as tracked
  async setTokenTracked(address: string, isTracked: boolean): Promise<boolean> {
    try {
      const existingStatistics = await this.findByAddress(address);
      
      if (!existingStatistics) {
        return false;
      }
      
      const updateQuery = `
        ALTER TABLE token_statistics
        UPDATE is_tracked = ${isTracked ? 1 : 0}
        WHERE contract_address = '${address}'
      `;
      
      await this.client.query({
        query: updateQuery
      });
      
      return true;
    } catch (error) {
      logger.error('Error setting token tracked status:', error);
      throw error;
    }
  }
  
  // Search tokens by name (assuming token name is part of the data)
  async searchTokens(searchTerm: string, page: number = 1, limit: number = 20): Promise<{statistics: TokenStatistics[], total: number}> {
    try {
      // This would require joining with the token_info table, assuming it contains the token name
      // For demonstration, we'll just search by address
      const offset = (page - 1) * limit;
      
      const query = `
        SELECT ts.* FROM token_statistics ts
        WHERE ts.contract_address LIKE '%${searchTerm}%'
        LIMIT ${limit} OFFSET ${offset}
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const statistics = await result.json<TokenStatistics[]>();
      
      // Get total count
      const countQuery = `
        SELECT count() as total FROM token_statistics
        WHERE contract_address LIKE '%${searchTerm}%'
      `;
      
      const countResult = await this.client.query({
        query: countQuery,
        format: 'JSONEachRow'
      });
      
      const countData = await countResult.json<[{ total: number }]>();
      const total = countData[0]?.total || 0;
      
      return { statistics, total };
    } catch (error) {
      logger.error('Error searching token statistics:', error);
      throw error;
    }
  }
}