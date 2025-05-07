import { ClickHouseClient } from '@clickhouse/client';
import { Pair } from '../types/database';
import { logger } from '../config/logger';
import * as fs from 'fs';
import * as path from 'path';

export class PairInfoQueue {
  private static instance: PairInfoQueue;
  private queue: Pair[] = [];
  private isProcessing: boolean = false;
  private flushInterval: NodeJS.Timeout;
  private client: ClickHouseClient | null = null;
  private readonly FLUSH_THRESHOLD = 10;
  private readonly PERSISTENCE_FILE = path.join(process.cwd(), 'pair_info_queue_backup.json');
  
  private constructor() {
    this.flushInterval = setInterval(() => this.flushQueue(), 5000);
    this.loadPersistedPairs();
    
    setInterval(() => this.persistPairs(), 60000);
  }

  public static getInstance(): PairInfoQueue {
    if (!PairInfoQueue.instance) {
      PairInfoQueue.instance = new PairInfoQueue();
    }
    return PairInfoQueue.instance;
  }

  public setClient(client: ClickHouseClient) {
    this.client = client;
  }

  public async addToQueue(pair: Omit<Pair, 'pair_id'>) {
    const pairWithId = await this.assignPairId(pair);
    this.queue.push(pairWithId);
    
    if (this.queue.length >= this.FLUSH_THRESHOLD && !this.isProcessing) {
      this.flushQueue();
    }
  }

  public async addManyToQueue(pairs: Array<Omit<Pair, 'pair_id'>>) {
    if (pairs.length === 0) return;
    
    const pairsWithIds = await Promise.all(pairs.map(pair => this.assignPairId(pair)));
    this.queue.push(...pairsWithIds);
    logger.info(`Added ${pairs.length} pairs to queue (total size: ${this.queue.length})`);
    
    if (this.queue.length >= this.FLUSH_THRESHOLD && !this.isProcessing) {
      this.flushQueue();
    }
  }

  private async assignPairId(pair: Omit<Pair, 'pair_id'>): Promise<Pair> {
    if (!this.client) {
      throw new Error('ClickHouse client not initialized');
    }
  
    try {
     
      const query = `
        SELECT MAX(pair_id) as max_id
        FROM pair
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const data = await result.json<Array<{max_id: number | null}>>();
      
      let maxId = 0;
      if (data.length > 0 && data[0].max_id !== null) {
        maxId = Number(data[0].max_id);
      }
      
      
      return {
        ...pair,
        pair_id: maxId + 1
      };
    } catch (error) {
      logger.error('Error assigning pair_id:', { error, service: "Cherry-OHLCV" });
      throw error;
    }
  }
  private async persistPairs() {
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
      
      logger.info(`Persisted ${this.queue.length} pairs to disk`);
    } catch (error) {
      logger.error('Failed to persist pair queue:', { error, service: "Cherry-OHLCV" });
    }
  }

  private async loadPersistedPairs() {
    try {
      if (fs.existsSync(this.PERSISTENCE_FILE)) {
        const data = await fs.promises.readFile(this.PERSISTENCE_FILE, 'utf8');
        const parsed = JSON.parse(data);
        
        if (parsed.queue && Array.isArray(parsed.queue)) {
          this.queue = parsed.queue;
        }
        
        logger.info(`Loaded ${this.queue.length} persisted pairs from disk`);
      }
    } catch (error) {
      logger.error('Failed to load persisted pairs:', { error, service: "Cherry-OHLCV" });
    }
  }

  private async flushQueue() {
    if (this.queue.length === 0 || this.isProcessing || !this.client) {
      return;
    }

    try {
      this.isProcessing = true;

      const pairsToProcess = [...this.queue];
      this.queue = [];

      logger.info(`Processing ${pairsToProcess.length} pairs`);

      const pairIds = pairsToProcess.map(p => p.pair_id).join(',');
      const query = `
        SELECT pair_id 
        FROM pair
        WHERE pair_id IN (${pairIds})
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const existingPairIds = new Set((await result.json<{pair_id: number}[]>())
        .map(item => item.pair_id));
      
      const newPairs = pairsToProcess.filter(p => !existingPairIds.has(p.pair_id));
      
      if (newPairs.length === 0) {
        logger.info('All pairs already exist, nothing to insert');
        return;
      }

      await this.client.insert({
        table: 'pair',
        values: newPairs,
        format: 'JSONEachRow'
      });
      
      logger.info(`Successfully inserted ${newPairs.length} pairs`);
      
    } catch (error) {
      logger.error('Error flushing pair queue:', { error, service: "Cherry-OHLCV" });
      
      
      const pairsToProcess = [...this.queue];
      this.queue = [...pairsToProcess, ...this.queue];
      
    } finally {
      this.isProcessing = false;
    }
  }

  public shutdown() {
    clearInterval(this.flushInterval);
    
    this.persistPairs();
    
    if (this.queue.length > 0 && this.client) {
      this.flushQueue();
    }
    
    logger.info(`Pair queue shut down. ${this.queue.length} pairs remaining in queue and persisted to disk.`);
  }

  public getStats() {
    return {
      queueSize: this.queue.length,
      isProcessing: this.isProcessing
    };
  }
}

export class PairModel {
  private client: ClickHouseClient;
  private queue: PairInfoQueue;

  constructor(client: ClickHouseClient) {
    this.client = client;
    this.queue = PairInfoQueue.getInstance();
    this.queue.setClient(client);
  }

  async findById(pairId: number): Promise<Pair | null> {
    try {
      const query = `
        SELECT * FROM pair
        WHERE pair_id = ${pairId}
        LIMIT 1
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const data = await result.json<Pair[]>();
      return data.length > 0 ? data[0] : null;
    } catch (error) {
      logger.error('Error in findById:', error);
      throw error;
    }
  }

  async findByTokenId(tokenId: number, params: { page?: number; limit?: number } = {}): Promise<Pair[]> {
    try {
      const { page = 1, limit = 20 } = params;
      const offset = (page - 1) * limit;
      
      const query = `
        SELECT * FROM pair
        WHERE token0_id = ${tokenId} OR token1_id = ${tokenId}
        ORDER BY created_date DESC
        LIMIT ${limit} OFFSET ${offset}
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      return await result.json<Pair[]>();
    } catch (error) {
      logger.error('Error in findByTokenId:', error);
      throw error;
    }
  }

  async createPairInfo(pairData: Omit<Pair, 'pair_id'>): Promise<void> {
    await this.queue.addToQueue(pairData);
  }

  async bulkCreatePairInfos(pairs: Array<Omit<Pair, 'pair_id'>>): Promise<void> {
    await this.queue.addManyToQueue(pairs);
  }

  getQueueStats() {
    return this.queue.getStats();
  }

  static initializeQueue(client: ClickHouseClient): void {
    const queue = PairInfoQueue.getInstance();
    queue.setClient(client);
    logger.info('Pair info queue initialized');
    
    process.on('SIGINT', () => {
      queue.shutdown();
    });
    
    process.on('SIGTERM', () => {
      queue.shutdown();
    });
  }

  async findPairsByPage(page: number = 1, limit: number = 20): Promise<{pairs: Pair[], total: number}> {
    try {
      const offset = (page - 1) * limit;
      
      const query = `
        SELECT * FROM pair
        ORDER BY created_date DESC
        LIMIT ${limit} OFFSET ${offset}
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const pairs = await result.json<Pair[]>();
      
      // Get total count
      const countQuery = `
        SELECT count() as total FROM pair
      `;
      
      const countResult = await this.client.query({
        query: countQuery,
        format: 'JSONEachRow'
      });
      
      const countData = await countResult.json<[{ total: number }]>();
      const total = countData[0]?.total || 0;
      
      return { pairs, total };
    } catch (error) {
      logger.error('Error fetching pairs by page:', error);
      throw error;
    }
  }

  async updatePairInfo(pairId: number, pairData: Partial<Pair>): Promise<boolean> {
    try {
   
      const existingPair = await this.findById(pairId);
      
      if (!existingPair) {
        return false;
      }
  
      const updateFields = [];
  
      
      if (pairData.contract_address !== undefined) updateFields.push(`contract_address = '${pairData.contract_address}'`);
      if (pairData.base_token !== undefined) updateFields.push(`base_token = '${pairData.base_token}'`);
      if (pairData.quote_token !== undefined) updateFields.push(`quote_token = '${pairData.quote_token}'`);
      
      if (updateFields.length === 0) {
        logger.info('No fields to update for pair', { pairId });
        return true;
      }
  
      const query = `
        ALTER TABLE pair
        UPDATE ${updateFields.join(', ')}
        WHERE pair_id = ${pairId}
      `;
      
      await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      logger.info(`Successfully updated pair with ID ${pairId}`);
      return true;
    } catch (error) {
      logger.error('Error updating pair info:', { error, pairId, service: "Cherry-OHLCV" });
      throw error;
    }
  }
  
  async deletePairInfo(pairId: number): Promise<boolean> {
    try {
     
      const existingPair = await this.findById(pairId);
      
      if (!existingPair) {
        return false;
      }
  
      const query = `
        ALTER TABLE pair
        DELETE WHERE pair_id = ${pairId}
      `;
      
      await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      logger.info(`Successfully deleted pair with ID ${pairId}`);
      return true;
    } catch (error) {
      logger.error('Error deleting pair info:', { error, pairId, service: "Cherry-OHLCV" });
      throw error; 
    }
  }
  
  async findTrendingPairs(page: number = 1, limit: number = 10): Promise<{pairs: Pair[], total: number}> {
    try {
      const offset = (page - 1) * limit;
      
      const query = `
        SELECT * FROM pair
        ORDER BY created_date DESC
        LIMIT ${limit} OFFSET ${offset}
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const pairs = await result.json<Pair[]>();
      
     
      const countQuery = `
        SELECT count() as total FROM pair
      `;
      
      const countResult = await this.client.query({
        query: countQuery,
        format: 'JSONEachRow'
      });
      
      const countData = await countResult.json<[{ total: number }]>();
      const total = countData[0]?.total || 0;
      
      return { pairs, total };
    } catch (error) {
      logger.error('Error fetching trending pairs:', { error, service: "Cherry-OHLCV" });
      throw error;
    }
  }
}