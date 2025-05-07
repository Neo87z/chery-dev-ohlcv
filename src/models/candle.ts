import { ClickHouseClient } from '@clickhouse/client';
import { Candle } from '../types/database';
import { CandleQueryParams } from '../types/api';
import { logger } from '../config/logger';

export class CandleModel {
  private client: ClickHouseClient;

  constructor(client: ClickHouseClient) {
    this.client = client;
  }

  async findByPairId(pairId: number, params: CandleQueryParams): Promise<Candle[]> {
    const { limit = 100, page = 1, from_date, to_date, timeframe = '1h', order = 'desc' } = params;
    const offset = (page - 1) * limit;
    
    try {
      let query = `
        SELECT * FROM candles_${timeframe}
        WHERE pair_id = ${pairId}
      `;
      
      if (from_date) {
        query += ` AND timestamp >= '${from_date}'`;
      }
      
      if (to_date) {
        query += ` AND timestamp <= '${to_date}'`;
      }
      
      query += `
        ORDER BY timestamp ${order}
        LIMIT ${limit} OFFSET ${offset}
      `;
      
      const result = await this.client.query({
        query,
        format: 'JSONEachRow'
      });
      
      return await result.json<Candle[]>();
    } catch (error) {
      logger.error('Error in findByPairId:', error);
      throw error;
    }
  }
}