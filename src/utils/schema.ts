import { ClickHouseClient } from '@clickhouse/client';
import { logger } from '../config/logger';

export async function createSchemaIfNotExists(client: ClickHouseClient): Promise<void> {
  logger.info('Setting up database schema');

  

  
}