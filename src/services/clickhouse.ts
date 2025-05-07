import { ClickHouseClient } from '@clickhouse/client';
import { logger } from '../config/logger';

export class ClickHouseService {
  private client: ClickHouseClient;
  
  constructor(client: ClickHouseClient) {
    this.client = client;
  }

  private async executeQueryInternal<T>(query: string): Promise<T[]> {
    const startTime = Date.now(); 
    try {
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });

      const data = await result.json<T[]>();
      const endTime = Date.now();
      logger.info(`Query executed in ${endTime - startTime}ms: ${query}`);
      return data;
    } catch (error) {
      const endTime = Date.now();
      logger.error('ClickHouse query error', {
        query,
        duration: endTime - startTime,
        error: error instanceof Error ? error.stack : error
      });
      throw new Error(`Failed to execute query: ${query}. Error: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }
  
  async executeQuery<T>(query: string): Promise<T[]> {
    return this.executeQueryInternal<T>(query);
  }
  
  async executeQuerySingle<T>(query: string): Promise<T | null> {
    try {
      const results = await this.executeQuery<T>(query);
      return results.length > 0 ? results[0] : null;
    } catch (error) {
      logger.error('ClickHouse query error (single):', { query, error });
      throw new Error(`Failed to execute query for single result: ${query}. Error: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }
  
  async countQuery(query: string): Promise<number> {
    try {
      const result = await this.executeQueryInternal<{ count: number }>(query);
      return result.length > 0 ? result[0].count : 0;
    } catch (error) {
      logger.error('ClickHouse count query error:', { query, error });
      throw new Error(`Failed to execute count query: ${query}. Error: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }
}
