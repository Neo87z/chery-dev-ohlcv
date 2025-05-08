import { createClient, ClickHouseClient, ClickHouseClientConfigOptions } from '@clickhouse/client';
import { logger } from './logger';

export let clickhouseClient: ClickHouseClient | null = null;

interface SetupClickhouseOptions {
  host?: string;
  username?: string;
  password?: string;
  clickhouse_settings?: Record<string, string | number>;
  request_timeout?: number;
  max_open_connections?: number;
  keep_alive_socket_ttl?: number;
}

export async function setupClickhouse(options: SetupClickhouseOptions = {}): Promise<ClickHouseClient> {
  if (clickhouseClient) {
    return clickhouseClient;
  }

  const {
    host =  'https://xy7dt4ybk1.eastus2.azure.clickhouse.cloud:8443',
    username =  'default',
    password = 'LxY5QpCs7q~pD',
    clickhouse_settings = {
      max_execution_time: 30,
      max_threads: 8,
      max_insert_threads: '8',
      max_memory_usage: '10000000000',
    },
    request_timeout = 30_000,
    max_open_connections = 100,
    keep_alive_socket_ttl = 60_000,
  } = options;

  const client = createClient({
    host,
    username,
    password,
    clickhouse_settings,
    compression: {
      request: true,
      response: true,
    },
    keep_alive: {
      enabled: true,
      socket_ttl: keep_alive_socket_ttl,
    },
    request_timeout,
    max_open_connections,
  });

  try {
    await client.ping();
    logger.info('✅ ClickHouse connection established successfully');
    clickhouseClient = client;
    return client;
  } catch (error: any) {
    const isNetworkError = error?.code === 'ECONNREFUSED' || error?.message?.includes('ENOTFOUND');
    const isAuthError = error?.message?.includes('authentication') || error?.message?.includes('password');

    if (isNetworkError) {
      logger.error('Network error while connecting to ClickHouse. Please check your host URL and internet connection.', { error });
    } else if (isAuthError) {
      logger.error(' Authentication failed for ClickHouse. Please verify your credentials.', { error });
    } else {
      logger.error(' Failed to connect to ClickHouse due to an unexpected error.', { error });
    }

    throw new Error(`ClickHouse connection failed: ${error.message}`);
  }
}
// Helper function to check if ClickHouse connection is healthy
export async function isClickHouseHealthy(): Promise<boolean> {
  try {
    if (!clickhouseClient) {
      return false;
    }

    await clickhouseClient.ping();
    return true;
  } catch (error) {
    logger.warn('ClickHouse health check failed', { error });
    return false;
  }
}