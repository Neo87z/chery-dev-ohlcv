import os from 'os';
import { ClickHouseClient } from '@clickhouse/client';
import { RedisClientType } from 'redis';
import { logger } from '../config/logger';

export interface SystemHealth {
  health: { status: string; };
  postgres: { status: string; queueStats: { queueSize: number; isProcessing: boolean; isClickHouse: boolean; }; };
  websocket: { activeConnections: number; };
  status: 'healthy' | 'degraded' | 'unhealthy';
  timestamp: string;
  uptime: string;
  system: {
    cpu: {
      loadAvg: number[];
      cores: number;
    };
    memory: {
      total: string;
      free: string;
      used: string;
      percentUsed: string;
    };
  };
  services: {
    clickhouse: {
      status: 'connected' | 'error';
      latency?: string;
    };
    redis?: {
      status: 'connected' | 'error';
      latency?: string;
    };
  };
}

export class HealthCheckService {
  private clickhouseClient: ClickHouseClient;
  private redisClient?: RedisClientType;

  constructor(clickhouseClient: ClickHouseClient, redisClient?: RedisClientType) {
    this.clickhouseClient = clickhouseClient;
    this.redisClient = redisClient;
  }

  private async checkClickhouse(): Promise<{ status: 'connected' | 'error'; latency?: number }> {
    try {
      const startTime = Date.now();
      await this.clickhouseClient.query({ query: 'SELECT 1', format: 'JSONEachRow' });
      return {
        status: 'connected' as const,
        latency: Date.now() - startTime
      };
    } catch (error) {
      logger.error('ClickHouse health check failed:', { error: error instanceof Error ? error.stack : error });
      return { status: 'error' as const };
    }
  }

  private async checkRedis(): Promise<{ status: 'connected' | 'error'; latency?: number }> {
    if (!this.redisClient) {
     
      return { status: 'error' as const };
    }

    try {
      const startTime = Date.now();
      await this.redisClient.ping();
      return {
        status: 'connected' as const,
        latency: Date.now() - startTime
      };
    } catch (error) {
      logger.error('Redis health check failed:', { error: error instanceof Error ? error.stack : error });
      return { status: 'error' as const };
    }
  }

  private formatBytes(bytes: number): string {
    if (bytes >= 1024 ** 3) {
      return (bytes / 1024 ** 3).toFixed(2) + ' GB';
    } else if (bytes >= 1024 ** 2) {
      return (bytes / 1024 ** 2).toFixed(2) + ' MB';
    } else if (bytes >= 1024) {
      return (bytes / 1024).toFixed(2) + ' KB';
    } else {
      return bytes + ' Bytes';
    }
  }

  private formatDuration(seconds: number): string {
    const minutes = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return `${minutes} minutes ${secs} seconds`;
  }

  async check(): Promise<SystemHealth> {
    const now = Date.now();

  
    const [clickhouseStatus, redisStatus] = await Promise.all([
      this.checkClickhouse(),
      this.checkRedis(),
    ]);

    const totalMemory = os.totalmem();  
    const freeMemory = os.freemem();    
    const usedMemory = totalMemory - freeMemory;  

   
    const totalMemoryFormatted = this.formatBytes(totalMemory);
    const freeMemoryFormatted = this.formatBytes(freeMemory);
    const usedMemoryFormatted = this.formatBytes(usedMemory);

   
    const health: SystemHealth = {
      status: clickhouseStatus.status === 'connected' ? 'healthy' : 'unhealthy',
      timestamp: new Date().toISOString(),
      uptime: this.formatDuration(process.uptime()),
      system: {
        cpu: {
          loadAvg: os.loadavg(),
          cores: os.cpus().length
        },
        memory: {
          total: totalMemoryFormatted,
          free: freeMemoryFormatted,
          used: usedMemoryFormatted,
          percentUsed: ((usedMemory / totalMemory) * 100).toFixed(2) + '%'
        }
      },
      services: {
        clickhouse: {
          status: clickhouseStatus.status,
          latency: clickhouseStatus.latency ? `${(clickhouseStatus.latency / 1000).toFixed(2)} seconds` : 'N/A'
        }
      },
      websocket: {
        activeConnections: 0
      },
      postgres: {
        status: '',
        queueStats: {
          queueSize: 0,
          isProcessing: false,
          isClickHouse: false
        }
      },
      health: {
        status: ''
      }
    };


    if (redisStatus) {
      health.services.redis = {
        status: redisStatus.status,
        latency: redisStatus.latency ? `${(redisStatus.latency / 1000).toFixed(2)} seconds` : 'N/A'
      };
    }

    return health;
  }
}
