import { ClickHouseClient } from '@clickhouse/client';
import { Trade } from '../types/database';
import { TradeQueryParams } from '../types/api';
import { logger } from '../config/logger';

interface OHLCV {
  timestamp: string;
  open_price: string;
  high_price: string;
  low_price: string;
  close_price: string;
  volume: string;
  trade_count: number;
  timeframe: string;
}

export interface OHLCVQueryParams {
  contract_address: string;
  limit?: number;
  page?: number;
  from_date?: string;
  to_date?: string;
  order?: 'asc' | 'desc';
  timeframe?: string;
}

class TradeQueue {
  private static instance: TradeQueue;
  private queue: Omit<Trade, 'trade_id'>[] = [];
  private backupQueue: Omit<Trade, 'trade_id'>[] = [];
  private isProcessing: boolean = false;
  private isMerging: boolean = false;
  private flushInterval: NodeJS.Timeout;
  private retryInterval: NodeJS.Timeout;
  private memoryCheckInterval: NodeJS.Timeout;
  private mergerInterval: NodeJS.Timeout;
  private client: ClickHouseClient | null = null;
  private connectionFailed: boolean = false;
  private connectionAttempts: number = 0;
  private readonly FLUSH_THRESHOLD = 5000;
  private readonly RETRY_DELAY_MS = 5000;
  private readonly CHUNK_SIZE = 2000;
  private readonly RETRY_CHUNK_SIZE = 1000;
  private readonly MAX_QUEUE_SIZE = 100000; // Reduced for Heroku memory constraints

  private tradesInserted: number = 0;
  private tradesAttempted: number = 0;
  private tradesFailed: number = 0;
  private lastResetTime: number = Date.now();
  private insertTimes: number[] = [];
  private readonly METRICS_WINDOW_MS = 60000;

  private lastLogTime: { [key: string]: number } = {};
  private readonly LOG_THROTTLE_MS = 5000;

  private constructor() {
    this.flushInterval = setInterval(() => this.flushQueue(), 1000);
    this.retryInterval = setInterval(() => this.retryFailedInsertions(), this.RETRY_DELAY_MS);
    this.memoryCheckInterval = setInterval(() => this.checkMemoryUsage(), 30000);
    this.mergerInterval = setInterval(() => this.runMergerScript(), 30000);

    setInterval(() => this.resetOldMetrics(), 10000);
  }

  public static getInstance(): TradeQueue {
    if (!TradeQueue.instance) {
      TradeQueue.instance = new TradeQueue();
    }
    return TradeQueue.instance;
  }

  public setClient(client: ClickHouseClient) {
    this.client = client;
    this.connectionFailed = false;
    this.connectionAttempts = 0;
  }

  private resetOldMetrics() {
    const now = Date.now();
    this.insertTimes = this.insertTimes.filter(time => (now - time) <= this.METRICS_WINDOW_MS);
    if (now - this.lastResetTime > this.METRICS_WINDOW_MS) {
      this.lastResetTime = now;
      this.tradesAttempted = 0;
      this.tradesFailed = 0;
    }
  }

  private throttledLog(level: 'info' | 'warn' | 'error', message: string, metadata?: any): void {
    const logKey = `${level}:${message}`;
    const now = Date.now();
    if (!this.lastLogTime[logKey] || now - this.lastLogTime[logKey] > this.LOG_THROTTLE_MS) {
      this.lastLogTime[logKey] = now;
      if (level === 'info') {
        logger.info(message, metadata || { service: "Cherry-OHLCV" });
      } else if (level === 'warn') {
        logger.warn(message, metadata || { service: "Cherry-OHLCV" });
      } else {
        logger.error(message, metadata || { service: "Cherry-OHLCV" });
      }
    }
  }

  public addToQueue(trade: Omit<Trade, 'trade_id'>) {
    const totalQueueSize = this.queue.length + this.backupQueue.length;
    if (totalQueueSize >= this.MAX_QUEUE_SIZE) {
      this.makeSpaceInQueue(1);
    }
    this.queue.push(trade);
  }

  private makeSpaceInQueue(requiredSpace: number) {
    const totalQueueSize = this.queue.length + this.backupQueue.length;
    if (totalQueueSize >= this.MAX_QUEUE_SIZE) {
      const spaceNeeded = Math.max(requiredSpace, Math.ceil(this.MAX_QUEUE_SIZE * 0.1));
      if (this.queue.length > spaceNeeded * 2) {
        this.throttledLog('warn', `Making space in queue by dropping ${spaceNeeded} oldest trades from main queue`);
        this.queue = this.queue.slice(spaceNeeded);
      } else if (this.backupQueue.length > spaceNeeded) {
        this.throttledLog('warn', `Making space in queue by dropping ${spaceNeeded} oldest trades from backup queue`);
        this.backupQueue = this.backupQueue.slice(spaceNeeded);
      } else {
        const mainQueuePortion = Math.ceil((this.queue.length / totalQueueSize) * spaceNeeded);
        const backupQueuePortion = spaceNeeded - mainQueuePortion;
        if (mainQueuePortion > 0 && this.queue.length > mainQueuePortion) {
          this.queue = this.queue.slice(mainQueuePortion);
        }
        if (backupQueuePortion > 0 && this.backupQueue.length > backupQueuePortion) {
          this.backupQueue = this.backupQueue.slice(backupQueuePortion);
        }
        this.throttledLog('warn', `Made space by dropping ${mainQueuePortion} trades from main queue and ${backupQueuePortion} from backup queue`);
      }
    }
  }

  public addManyToQueue(trades: Omit<Trade, 'trade_id'>[]) {
    if (trades.length === 0) return;
    const totalQueueSize = this.queue.length + this.backupQueue.length;
    const availableSpace = this.MAX_QUEUE_SIZE - totalQueueSize;
    if (trades.length > availableSpace) {
      this.makeSpaceInQueue(trades.length);
      this.throttledLog('warn', `Made space for ${trades.length} new trades in the queue`);
    }
    this.queue.push(...trades);
    this.throttledLog('info', `Added ${trades.length} trades to queue (total size: ${this.queue.length + this.backupQueue.length})`);
  }

  public canAcceptMoreTrades(): boolean {
    return (this.queue.length + this.backupQueue.length) < this.MAX_QUEUE_SIZE * 0.9;
  }

  private checkMemoryUsage() {
    const memUsage = process.memoryUsage();
    const heapUsedPercentage = (memUsage.heapUsed / memUsage.heapTotal) * 100;
    if (heapUsedPercentage > 85) {
      this.throttledLog('warn', `High memory usage detected: ${heapUsedPercentage.toFixed(2)}%. Reducing queue size.`);
      const keepSize = Math.floor(this.queue.length * 0.7);
      this.queue = this.queue.slice(-keepSize);
      const keepBackupSize = Math.floor(this.backupQueue.length * 0.7);
      this.backupQueue = this.backupQueue.slice(-keepBackupSize);
      this.throttledLog('warn', `Reduced queue sizes to: main=${this.queue.length}, backup=${this.backupQueue.length}`);
    }
  }

  private async runMergerScript() {
    this.isMerging = false;
  }

  private async flushQueue() {
    if (this.queue.length === 0 || this.isProcessing || this.connectionFailed || this.isMerging) {
      return;
    }
    try {
      this.isProcessing = true;
      const tradesToProcess = this.queue.slice(0, this.CHUNK_SIZE);
      this.queue = this.queue.slice(this.CHUNK_SIZE);
      this.throttledLog('warn', `Flushing queue with ${tradesToProcess.length} trades (${this.queue.length} remaining)`);
      if (!this.client) {
        throw new Error('ClickHouse client not initialized');
      }
      try {
        await this.client.query({ query: 'SELECT 1', format: 'JSONEachRow' });
        this.connectionFailed = false;
        this.connectionAttempts = 0;
      } catch (pingError) {
        throw pingError;
      }
      const startTime = Date.now();
      this.tradesAttempted += tradesToProcess.length;
      await this.client.insert({
        table: 'trade',
        values: tradesToProcess,
        format: 'JSONEachRow'
      });
      const endTime = Date.now();
      this.tradesInserted += tradesToProcess.length;
      this.insertTimes.push(endTime);
      if (tradesToProcess.length > 1000) {
        const duration = endTime - startTime;
        const tradesPerSecond = tradesToProcess.length / (duration / 1000);
        this.throttledLog('info', `Inserted ${tradesToProcess.length} trades in ${duration}ms (${tradesPerSecond.toFixed(2)} trades/sec)`, {
          service: "Cherry-OHLCV",
          performance: true
        });
      }
      if (this.queue.length > 0) {
        setImmediate(() => this.flushQueue());
      }
    } catch (error) {
      this.throttledLog('error', 'Error flushing trade queue:', { error, service: "Cherry-OHLCV" });
      this.tradesFailed += this.queue.slice(0, this.CHUNK_SIZE).length;
      const failedTrades = this.queue.slice(0, this.CHUNK_SIZE);
      if (this.backupQueue.length + failedTrades.length > this.MAX_QUEUE_SIZE - this.queue.length + failedTrades.length) {
        this.makeSpaceInQueue(failedTrades.length);
      }
      this.backupQueue.push(...failedTrades);
      this.queue = this.queue.slice(this.CHUNK_SIZE);
      this.connectionFailed = true;
      this.connectionAttempts++;
      const backoffDelay = this.RETRY_DELAY_MS * Math.min(Math.pow(2, this.connectionAttempts - 1), 60);
      this.throttledLog('warn', `Connection failed, will retry in ${backoffDelay/1000} seconds (attempt ${this.connectionAttempts})`);
      setTimeout(() => {
        this.connectionFailed = false;
        this.throttledLog('warn', 'Resetting connection failed status, will attempt to reconnect');
      }, backoffDelay);
    } finally {
      this.isProcessing = false;
    }
  }

  private async retryFailedInsertions() {
    if (this.backupQueue.length === 0 || this.isProcessing || this.connectionFailed || this.isMerging) {
      return;
    }
    try {
      this.isProcessing = true;
      const tradesToRetry = this.backupQueue.slice(0, this.RETRY_CHUNK_SIZE);
      this.backupQueue = this.backupQueue.slice(this.RETRY_CHUNK_SIZE);
      this.throttledLog('warn', `Retrying insertion of ${tradesToRetry.length} trades (${this.backupQueue.length} waiting in backup queue)`);
      if (!this.client) {
        throw new Error('ClickHouse client not initialized');
      }
      await this.client.query({ query: 'SELECT 1', format: 'JSONEachRow' });
      const startTime = Date.now();
      this.tradesAttempted += tradesToRetry.length;
      await this.client.insert({
        table: 'trade',
        values: tradesToRetry,
        format: 'JSONEachRow'
      });
      const endTime = Date.now();
      this.tradesInserted += tradesToRetry.length;
      this.insertTimes.push(endTime);
      this.connectionFailed = false;
      this.connectionAttempts = 0;
      if (this.backupQueue.length > 0) {
        setImmediate(() => this.retryFailedInsertions());
      }
    } catch (error) {
      this.throttledLog('error', 'Error retrying failed insertions:', { error, service: "Cherry-OHLCV" });
      this.tradesFailed += this.backupQueue.slice(0, this.RETRY_CHUNK_SIZE).length;
      const failedRetries = this.backupQueue.slice(0, this.RETRY_CHUNK_SIZE);
      this.makeSpaceInQueue(failedRetries.length);
      this.backupQueue = [...failedRetries, ...this.backupQueue];
      this.connectionFailed = true;
      this.connectionAttempts++;
      const backoffDelay = this.RETRY_DELAY_MS * Math.min(Math.pow(2, this.connectionAttempts - 1), 60);
      this.throttledLog('warn', `Retry failed, will attempt again in ${backoffDelay/1000} seconds (attempt ${this.connectionAttempts})`);
    } finally {
      this.isProcessing = false;
    }
  }

  public shutdown() {
    clearInterval(this.flushInterval);
    clearInterval(this.retryInterval);
    clearInterval(this.memoryCheckInterval);
    clearInterval(this.mergerInterval);
    if (this.queue.length > 0 && !this.connectionFailed && !this.isMerging) {
      this.flushQueue();
    }
    logger.warn(`Trade queue shut down. ${this.queue.length + this.backupQueue.length} trades remaining in queue.`, { service: "Cherry-OHLCV" });
  }

  public getStats() {
    const now = Date.now();
    const recentInsertions = this.insertTimes.filter(time => (now - time) <= this.METRICS_WINDOW_MS).length;
    const tradeRate = recentInsertions / (this.METRICS_WINDOW_MS / 1000);
    const errorRate = this.tradesAttempted > 0 ? (this.tradesFailed / this.tradesAttempted) * 100 : 0;
    let runtime = 0;
    if (this.insertTimes.length > 0) {
      const oldestTime = Math.min(...this.insertTimes);
      runtime = (now - oldestTime) / 1000;
      if (runtime < 0.01) runtime = 0.01;
    }
    return {
      mainQueueSize: this.queue.length,
      retryQueueSize: this.backupQueue.length,
      totalSize: this.queue.length + this.backupQueue.length,
      utilization: ((this.queue.length + this.backupQueue.length) / this.MAX_QUEUE_SIZE) * 100,
      connectionStatus: this.connectionFailed ? 'failed' : 'ok',
      retryAttempts: this.connectionAttempts,
      mergerStatus: this.isMerging ? 'running' : 'idle',
      tradesInserted: this.tradesInserted,
      tradesAttempted: this.tradesAttempted,
      tradesFailed: this.tradesFailed,
      tradeRate: parseFloat(tradeRate.toFixed(2)),
      errorRate: parseFloat(errorRate.toFixed(2)),
      errorPercentage: `${errorRate.toFixed(2)}%`,
      runtimeSeconds: runtime.toFixed(2),
      performance: {
        tradesPerSecond: parseFloat(tradeRate.toFixed(2)),
        totalTradesInserted: this.tradesInserted,
        totalTradesFailed: this.tradesFailed,
        errorRate: `${errorRate.toFixed(2)}%`,
        runtime: `${runtime.toFixed(2)} seconds`
      }
    };
  }
}

export class TradeModel {
  private client: ClickHouseClient;
  private queue: TradeQueue;

  constructor(client: ClickHouseClient) {
    this.client = client;
    this.queue = TradeQueue.getInstance();
    this.queue.setClient(client);
  }

  async findByPairId(pairId: number, params: TradeQueryParams = {}): Promise<Trade[]> {
    const { limit = 100, page = 1, from_date, to_date, order = 'desc' } = params;
    const offset = (page - 1) * limit;
    try {
      let query = `
        SELECT * FROM trade
        WHERE pair_id = ${pairId}
      `;
      if (from_date) {
        query += ` AND trade_timestamp >= '${from_date}'`;
      }
      if (to_date) {
        query += ` AND trade_timestamp <= '${to_date}'`;
      }
      query += `
        ORDER BY trade_timestamp ${order}
        LIMIT ${limit} OFFSET ${offset}
      `;
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      return await result.json<Trade[]>();
    } catch (error) {
      logger.error('Error in findByPairId:', error);
      throw error;
    }
  }

  async findAll(params: TradeQueryParams = {}): Promise<Trade[]> {
    const { limit = 100, page = 1, from_date, to_date, order = 'desc', trade_type } = params;
    const offset = (page - 1) * limit;
    try {
      let query = `
        SELECT * FROM trade
        WHERE 1=1
      `;
      const queryParams: any = {};
      if (trade_type) {
        query += ` AND trade_type = {trade_type:String}`;
        queryParams.trade_type = trade_type;
      }
      if (from_date) {
        query += ` AND trade_timestamp >= {from_date:String}`;
        queryParams.from_date = from_date;
      }
      if (to_date) {
        query += ` AND trade_timestamp <= {to_date:String}`;
        queryParams.to_date = to_date;
      }
      query += `
        ORDER BY trade_timestamp ${order}
        LIMIT ${limit} OFFSET ${offset}
      `;
      const result = await this.client.query({
        query,
        format: 'JSONEachRow',
        query_params: queryParams
      });
      return await result.json<Trade[]>();
    } catch (error) {
      logger.error('Error in findAll:', error);
      throw error;
    }
  }

  async getOHLCV(params: OHLCVQueryParams): Promise<OHLCV[]> {
    const { 
      contract_address, 
      limit, 
      page = 1, 
      from_date, 
      to_date, 
      order = 'asc', 
      timeframe 
    } = params;
    
    const offset = page > 1 ? (page - 1) * (limit || 0) : 0;
    
    // Define all available timeframes
    const timeframes = [
      { table: 'candles_1s', timeframe: '1s' },
      { table: 'candles_1m', timeframe: '1m' },
      { table: 'candles_15m', timeframe: '15m' },
      { table: 'candles_1h', timeframe: '1h' },
      { table: 'candles_4h', timeframe: '4h' },
      { table: 'candles_1d', timeframe: '1d' }
    ];
    
    // Only use the specified timeframe if provided, otherwise use all
    const targetTimeframes = timeframe 
      ? timeframes.filter(tf => tf.timeframe === timeframe) 
      : timeframes;
    
    const results: OHLCV[] = [];
    
    try {
      // If a specific timeframe is requested, only query that table
      for (const { table, timeframe: tf } of targetTimeframes) {
        let query = `
          SELECT 
            timestamp,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            trade_count,
            '${tf}' as timeframe
          FROM ${table}
          WHERE contract_address = {contract_address:String}
        `;
        
        const queryParams: Record<string, string> = { contract_address };
        
        if (from_date) {
          query += ` AND timestamp >= parseDateTimeBestEffort({from_date:String})`;
          queryParams.from_date = from_date;
        }
        
        if (to_date) {
          query += ` AND timestamp <= parseDateTimeBestEffort({to_date:String})`;
          queryParams.to_date = to_date;
        }
        
        query += ` ORDER BY timestamp ${order}`;
        
        if (limit) {
          query += ` LIMIT ${limit} OFFSET ${offset}`;
        }
        
        logger.debug('ClickHouse query:', query, 'params:', queryParams);
        
        const result = await this.client.query({
          query,
          format: 'JSONEachRow',
          query_params: queryParams
        });
        
        const candles = await result.json<OHLCV[]>();
        
        // Ensure no duplicates by checking if the timestamp already exists
        for (const candle of candles) {
          const timestamp = new Date(candle.timestamp).getTime();
          const isDuplicate = results.some(existing => 
            new Date(existing.timestamp).getTime() === timestamp
          );
          
          if (!isDuplicate) {
            results.push(candle);
          }
        }
      }
      
      // Sort all results by timestamp
      return results.sort((a, b) => {
        const timeA = new Date(a.timestamp).getTime();
        const timeB = new Date(b.timestamp).getTime();
        return order === 'asc' ? timeA - timeB : timeB - timeA;
      });
    } catch (error) {
      logger.error('Error in getOHLCV:', error);
      throw error;
    }
  }
  
  async queueTrade(trade: Omit<Trade, 'trade_id'> | Omit<Trade, 'trade_id'>[]): Promise<void> {
    if (Array.isArray(trade)) {
      this.queue.addManyToQueue(trade);
    } else {
      this.queue.addToQueue(trade);
    }
  }

  async queueTrades(trades: Omit<Trade, 'trade_id'>[]): Promise<void> {
    this.queue.addManyToQueue(trades);
  }

  getQueueStats() {
    const stats = this.queue.getStats();
    return {
      ...stats,
      formattedStats: `----- PERFORMANCE STATISTICS -----
Queue: ${stats.mainQueueSize} pending, ${stats.retryQueueSize} retry
Total trades inserted: ${stats.tradesInserted}
Current trade rate: ${stats.tradeRate.toFixed(2)} trades/sec
Error rate: ${stats.errorPercentage}
Connection: ${stats.connectionStatus}
Merger Status: ${stats.mergerStatus}
Utilization: ${stats.utilization.toFixed(2)}%
---------------------------------`
    };
  }

  canAcceptMoreTrades(): boolean {
    return this.queue.canAcceptMoreTrades();
  }

  static initializeQueue(client: ClickHouseClient): void {
    const queue = TradeQueue.getInstance();
    queue.setClient(client);
    logger.warn('Trade queue initialized with memory-optimized settings for Heroku deployment', { service: "Cherry-OHLCV" });
    
    // Add graceful shutdown handlers
    process.on('SIGINT', () => {
      queue.shutdown();
    });
    process.on('SIGTERM', () => {
      queue.shutdown();
    });
  }
}