import { ClickHouseClient } from '@clickhouse/client';
import { SocialInfo } from '../types/database';
import { logger } from '../config/logger';
import * as fs from 'fs';
import * as path from 'path';

export class SocialInfoQueue {
  private static instance: SocialInfoQueue;
  private queue: SocialInfo[] = [];
  private isProcessing: boolean = false;
  private flushInterval: NodeJS.Timeout;
  private client: ClickHouseClient | null = null;
  private readonly FLUSH_THRESHOLD = 10;
  private readonly PERSISTENCE_FILE = path.join(process.cwd(), 'social_info_queue_backup.json');
  
  private constructor() {
    this.flushInterval = setInterval(() => this.flushQueue(), 5000);
    this.loadPersistedSocials();
    
    setInterval(() => this.persistSocials(), 60000);
  }

  public static getInstance(): SocialInfoQueue {
    if (!SocialInfoQueue.instance) {
      SocialInfoQueue.instance = new SocialInfoQueue();
    }
    return SocialInfoQueue.instance;
  }

  public setClient(client: ClickHouseClient) {
    this.client = client;
  }

  public addToQueue(social: SocialInfo) {
    this.queue.push(social);
    
    if (this.queue.length >= this.FLUSH_THRESHOLD && !this.isProcessing) {
      this.flushQueue();
    }
  }

  public addManyToQueue(socials: SocialInfo[]) {
    if (socials.length === 0) return;
    
    this.queue.push(...socials);
    logger.info(`Added ${socials.length} social infos to queue (total size: ${this.queue.length})`);
    
    if (this.queue.length >= this.FLUSH_THRESHOLD && !this.isProcessing) {
      this.flushQueue();
    }
  }

  private async persistSocials() {
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
      
      logger.info(`Persisted ${this.queue.length} social infos to disk`);
    } catch (error) {
      logger.error('Failed to persist social queue:', { error, service: "Cherry-OHLCV" });
    }
  }

  private async loadPersistedSocials() {
    try {
      if (fs.existsSync(this.PERSISTENCE_FILE)) {
        const data = await fs.promises.readFile(this.PERSISTENCE_FILE, 'utf8');
        const parsed = JSON.parse(data);
        
        if (parsed.queue && Array.isArray(parsed.queue)) {
          this.queue = parsed.queue;
        }
        
        logger.info(`Loaded ${this.queue.length} persisted social infos from disk`);
      }
    } catch (error) {
      logger.error('Failed to load persisted social infos:', { error, service: "Cherry-OHLCV" });
    }
  }

  private async flushQueue() {
    if (this.queue.length === 0 || this.isProcessing || !this.client) {
      return;
    }

    try {
      this.isProcessing = true;

      const socialsToProcess = [...this.queue];
      this.queue = [];

      logger.info(`Processing ${socialsToProcess.length} social infos`);

      const addresses = socialsToProcess.map(t => t.contract_address).join(',');
      const query = `
        SELECT contract_address 
        FROM social_info
        WHERE contract_address IN (${addresses})
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const existingAddresses = new Set((await result.json<{contract_address: any}[]>())
        .map(item => item.contract_address));
      
      const newSocials = socialsToProcess.filter(t => !existingAddresses.has(t.contract_address));
      
      if (newSocials.length === 0) {
        logger.info('All social infos already exist, nothing to insert');
        return;
      }

  
      const socialsToInsert = newSocials.map(social => ({
        ...social,
       
        other_social: Array.isArray(social.other_social) ? social.other_social : 
          (social.other_social ? [social.other_social] : [])
      }));

      await this.client.insert({
        table: 'social_info',
        values: socialsToInsert,
        format: 'JSONEachRow'
      });
      
      logger.info(`Successfully inserted ${socialsToInsert.length} social infos`);
      
    } catch (error) {
      logger.error('Error flushing social queue:', { error, service: "Cherry-OHLCV" });
      
      
      this.queue = [...this.queue, ...this.queue];
      
    } finally {
      this.isProcessing = false;
    }
  }

  public shutdown() {
    clearInterval(this.flushInterval);
    
    this.persistSocials();
    
    if (this.queue.length > 0 && this.client) {
      this.flushQueue();
    }
    
    logger.info(`Social queue shut down. ${this.queue.length} social infos remaining in queue and persisted to disk.`);
  }

  public getStats() {
    return {
      queueSize: this.queue.length,
      isProcessing: this.isProcessing
    };
  }
}

export class SocialInfoModel {
 
  private client: ClickHouseClient;
  private queue: SocialInfoQueue;
  bulkCreateSocials: any;

  constructor(client: ClickHouseClient) {
    this.client = client;
    this.queue = SocialInfoQueue.getInstance();
    this.queue.setClient(client);
  }

  async findByAddress(address: string | number): Promise<SocialInfo | null> {
    try {
      const query = `
        SELECT * FROM social_info
        WHERE contract_address = ${address}
        LIMIT 1
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const data = await result.json<SocialInfo[]>();
      return data.length > 0 ? data[0] : null;
    } catch (error) {
      logger.error('Error in findByAddress:', error);
      throw error;
    }
  }

  async createSocialInfo(socialData: SocialInfo): Promise<void> {
    this.queue.addToQueue(socialData);
  }

  async bulkCreateSocialInfos(socials: SocialInfo[]): Promise<void> {
    this.queue.addManyToQueue(socials);
  }

  getQueueStats() {
    return this.queue.getStats();
  }

  static initializeQueue(client: ClickHouseClient): void {
    const queue = SocialInfoQueue.getInstance();
    queue.setClient(client);
    logger.info('Social info queue initialized');
    
    process.on('SIGINT', () => {
      queue.shutdown();
    });
    
    process.on('SIGTERM', () => {
      queue.shutdown();
    });
  }

  async findByPage(page: number = 1, limit: number = 20): Promise<{socials: SocialInfo[], total: number}> {
    try {
      const offset = (page - 1) * limit;
      
      const query = `
        SELECT * FROM social_info
        ORDER BY contract_address ASC
        LIMIT ${limit} OFFSET ${offset}
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      const socials = await result.json<SocialInfo[]>();
      
      // Get total count
      const countQuery = `
        SELECT count() as total FROM social_info
      `;
      
      const countResult = await this.client.query({
        query: countQuery,
        format: 'JSONEachRow'
      });
      
      const countData = await countResult.json<[{ total: number }]>();
      const total = countData[0]?.total || 0;
      
      return { socials, total };
    } catch (error) {
      logger.error('Error fetching socials by page:', error);
      throw error;
    }
  }

  async updateSocialInfo(address: string | number, socialData: Partial<SocialInfo>): Promise<boolean> {
    try {
     
      const existingSocial = await this.findByAddress(address);
      
      if (!existingSocial) {
        return false;
      }

      const updateFields = [];

      if (socialData.website) updateFields.push(`website = '${socialData.website}'`);
      if (socialData.telegram) updateFields.push(`telegram = '${socialData.telegram}'`);
      if (socialData.explorer) updateFields.push(`explorer = '${socialData.explorer}'`);
      if (socialData.github) updateFields.push(`github = '${socialData.github}'`);
      
  
      if (socialData.other_social && Array.isArray(socialData.other_social)) {
        const socialArray = socialData.other_social.map(social => `'${social}'`).join(', ');
        updateFields.push(`other_social = [${socialArray}]`);
      }

      if (updateFields.length === 0) {
        return false;
      }

      const updateQuery = `
        ALTER TABLE social_info
        UPDATE ${updateFields.join(', ')}
        WHERE contract_address = ${address}
      `;

      await this.client.query({
        query: updateQuery
      });

      return true;
    } catch (error) {
      logger.error('Error updating social info:', error);
      throw error;
    }
  }

  async deleteTokenInfo(address: string | number): Promise<boolean> {
    try {
      
      const existingSocial = await this.findByAddress(address);
      
      if (!existingSocial) {
        return false;
      }

      const deleteQuery = `
        ALTER TABLE social_info
        DELETE WHERE contract_address = ${address}
      `;

      await this.client.query({
        query: deleteQuery
      });

      return true;
    } catch (error) {
      logger.error('Error deleting social info:', error);
      throw error;
    }
  }
}