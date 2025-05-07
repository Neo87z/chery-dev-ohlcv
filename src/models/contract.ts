import { ClickHouseClient } from '@clickhouse/client';
import { ContractInfo } from '../types/database';
import { logger } from '../config/logger';
import * as fs from 'fs';
import * as path from 'path';

export class ContractQueue {
  private static instance: ContractQueue;
  private queue: Omit<ContractInfo, 'token_id' | 'created_at' | 'updated_at'>[] = [];
  private isProcessing: boolean = false;
  private flushInterval: NodeJS.Timeout;
  private client: ClickHouseClient | null = null;
  private lastTokenId: number = 0;
  private readonly FLUSH_THRESHOLD = 10;
  private readonly PERSISTENCE_FILE = path.join(process.cwd(), 'contract_queue_backup.json');
  
  private constructor() {
    this.flushInterval = setInterval(() => this.flushQueue(), 5000);
    this.loadPersistedContracts();
    
    setInterval(() => this.persistContracts(), 60000);
  }



  public static getInstance(): ContractQueue {
    if (!ContractQueue.instance) {
      ContractQueue.instance = new ContractQueue();
    }
    return ContractQueue.instance;
  }

  public setClient(client: ClickHouseClient) {
    this.client = client;
    this.initializeLastTokenId();
  }
  
  private async initializeLastTokenId(): Promise<void> {
   
  }

  // Fixed method to correctly increment the token ID
  private getNextTokenId(): number {
    // Make sure we're performing numeric addition by explicitly converting to Number
    this.lastTokenId = Number(this.lastTokenId) + 1;
    return this.lastTokenId;
  }

  public addToQueue(contract: Omit<ContractInfo, 'token_id' | 'created_at' | 'updated_at'>) {
    this.queue.push(contract);
    
    if (this.queue.length >= this.FLUSH_THRESHOLD && !this.isProcessing) {
      this.flushQueue();
    }
  }

  public addManyToQueue(contracts: Omit<ContractInfo, 'token_id' | 'created_at' | 'updated_at'>[]) {
    if (contracts.length === 0) return;
    
    this.queue.push(...contracts);
    logger.info(`Added ${contracts.length} contracts to queue (total size: ${this.queue.length})`);
    
    if (this.queue.length >= this.FLUSH_THRESHOLD && !this.isProcessing) {
      this.flushQueue();
    }
  }

  private async persistContracts() {
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
      
      logger.info(`Persisted ${this.queue.length} contracts to disk`);
    } catch (error) {
      logger.error('Failed to persist contract queue:', { error, service: "Cherry-OHLCV" });
    }
  }

  private async loadPersistedContracts() {
    try {
      if (fs.existsSync(this.PERSISTENCE_FILE)) {
        const data = await fs.promises.readFile(this.PERSISTENCE_FILE, 'utf8');
        const parsed = JSON.parse(data);
        
        if (parsed.queue && Array.isArray(parsed.queue)) {
          this.queue = parsed.queue;
        }
        
        logger.info(`Loaded ${this.queue.length} persisted contracts from disk`);
      }
    } catch (error) {
      logger.error('Failed to load persisted contracts:', { error, service: "Cherry-OHLCV" });
    }
  }

  private async flushQueue() {
    if (this.queue.length === 0 || this.isProcessing || !this.client) {
      return;
    }

    try {
      this.isProcessing = true;

      const contractsToProcess = [...this.queue];
      this.queue = [];

      logger.info(`Processing ${contractsToProcess.length} contracts`);

      const addresses = contractsToProcess.map(c => `'${c.contract_address}'`).join(',');
      const query = `
        SELECT contract_address 
        FROM contract_info
        WHERE contract_address IN (${addresses})
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const existingAddresses = new Set((await result.json<{contract_address: string}[]>())
        .map(item => item.contract_address));
      
      const newContracts = contractsToProcess.filter(c => !existingAddresses.has(c.contract_address));
      
      if (newContracts.length === 0) {
        logger.info('All contracts already exist, nothing to insert');
        return;
      }
      
      const now = new Date().toISOString();
      const contractsToInsert = newContracts.map(contract => ({
        ...contract,
        token_id: this.getNextTokenId(),
        created_at: now,
        updated_at: now
      }));

      await this.client.insert({
        table: 'contract_info',
        values: contractsToInsert,
        format: 'JSONEachRow'
      });
      
      logger.info(`Successfully inserted ${contractsToInsert.length} contracts`);
      
    } catch (error) {
      logger.error('Error flushing contract queue:', { error, service: "Cherry-OHLCV" });
      
      // Put the failed contracts back in the queue
      this.queue = [...this.queue, ...this.queue];
      
    } finally {
      this.isProcessing = false;
    }
  }

  public shutdown() {
    clearInterval(this.flushInterval);
    
    this.persistContracts();
    
    if (this.queue.length > 0 && this.client) {
      this.flushQueue();
    }
    
    logger.info(`Contract queue shut down. ${this.queue.length} contracts remaining in queue and persisted to disk.`);
  }

  public getStats() {
    return {
      queueSize: this.queue.length,
      lastTokenId: this.lastTokenId,
      isProcessing: this.isProcessing
    };
  }
}

export class ContractModel {
  private client: ClickHouseClient;
  private queue: ContractQueue;

  constructor(client: ClickHouseClient) {
    this.client = client;
    this.queue = ContractQueue.getInstance();
    this.queue.setClient(client);
  }

  async findByAddress(address: string): Promise<ContractInfo | null> {
  if (!address) {
    logger.warn('Cannot find contract with empty address');
    return null;
  }

  try {
    logger.info(`Looking up contract with address: ${address}`);
    
    const query = `
      SELECT * FROM contract_info
      WHERE contract_address = '${address}'
      LIMIT 1
    `;
    
    const result = await this.client.query({
      query,
      format: 'JSONEachRow'
    });
    
    const data = await result.json<ContractInfo[]>();
    
    if (data.length > 0) {
      logger.info(`Found existing contract for address: ${address}`);
      return data[0];
    } else {
      logger.info(`No contract found for address: ${address}`);
      return null;
    }
  } catch (error) {
    logger.error(`Error finding contract with address ${address}:`, error);
    throw error;
  }
}

 

  
  

  async createContract(contractData: Omit<ContractInfo, 'token_id' | 'created_at' | 'updated_at'>): Promise<void> {
    this.queue.addToQueue(contractData);
  }

  async bulkCreateContracts(contracts: Omit<ContractInfo, 'token_id' | 'created_at' | 'updated_at'>[]): Promise<void> {
    this.queue.addManyToQueue(contracts);
  }
 
  getQueueStats() {
    return this.queue.getStats();
  }

  static initializeQueue(client: ClickHouseClient): void {
    const queue = ContractQueue.getInstance();
    queue.setClient(client);
    logger.info('Contract queue initialized');
    
    process.on('SIGINT', () => {
      queue.shutdown();
    });
    
    process.on('SIGTERM', () => {
      queue.shutdown();
    });
  }
}