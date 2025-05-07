import { Router } from 'express';
import WebSocket from 'ws';
import { ClickHouseClient } from '@clickhouse/client';
import { TokenStatisticsModel } from '../models/token_stats';
import { logger } from '../config/logger';
import { setupClickhouse } from '../config/db';

enum MessageType {
  GET_TOKEN_STATISTICS = 'GET_TOKEN_STATISTICS',
  CREATE_TOKEN_STATISTICS = 'CREATE_TOKEN_STATISTICS',
  BULK_CREATE_TOKEN_STATISTICS = 'BULK_CREATE_TOKEN_STATISTICS',
  UPDATE_TOKEN_STATISTICS = 'UPDATE_TOKEN_STATISTICS',
  GET_QUEUE_STATUS = 'GET_QUEUE_STATUS',
  TOKEN_STATISTICS_CREATED = 'TOKEN_STATISTICS_CREATED',
  TOKEN_STATISTICS_UPDATED = 'TOKEN_STATISTICS_UPDATED',
  ERROR = 'ERROR',
}

interface WebSocketMessage {
  type: MessageType;
  payload: any;
  requestId?: string;
}

(async function initializeTokenStatisticsQueue() {
  try {
    const client = await setupClickhouse();
    TokenStatisticsModel.initializeQueue(client);
    logger.info('Token statistics queue initialized successfully');
  } catch (error) {
    logger.error('Failed to initialize token statistics queue:', error);
  }
})();

function validate(data: any, rules: any) {
  const errors: any[] = [];

  for (const [field, rule] of Object.entries(rules)) {
    const value = data[field];
    const { required, type } = rule as { required: boolean; type: string };

    if (required && (value === undefined || value === null || value === '')) {
      errors.push({ field, message: `${field} is required` });
    } else if (value !== undefined && typeof value !== type) {
      errors.push({ field, message: `${field} must be a ${type}` });
    }
  }

  return errors;
}

export class TokenStatisticsWebSocketHandler {
  private clickhouseClient: any;
  private tokenStatisticsModel!: TokenStatisticsModel;

  constructor() {
    this.initialize();
  }

  private async initialize() {
    try {
      this.clickhouseClient = await setupClickhouse();
      this.tokenStatisticsModel = new TokenStatisticsModel(this.clickhouseClient);
    } catch (error) {
      logger.error('Failed to initialize TokenStatisticsWebSocketHandler:', error);
    }
  }

  public async handleMessage(ws: WebSocket, message: string) {
    try {
      const parsedMessage: WebSocketMessage = JSON.parse(message);
      const { type, payload, requestId } = parsedMessage;

      switch (type) {
        case MessageType.GET_TOKEN_STATISTICS:
          await this.handleGetTokenStatistics(ws, payload, requestId);
          break;
        case MessageType.CREATE_TOKEN_STATISTICS:
          await this.handleCreateTokenStatistics(ws, payload, requestId);
          break;
        case MessageType.BULK_CREATE_TOKEN_STATISTICS:
          await this.handleBulkCreateTokenStatistics(ws, payload, requestId);
          break;
        case MessageType.UPDATE_TOKEN_STATISTICS:
          await this.handleUpdateTokenStatistics(ws, payload, requestId);
          break;
        case MessageType.GET_QUEUE_STATUS:
          await this.handleGetQueueStatus(ws, requestId);
          break;
        default:
          this.sendError(ws, 'Unknown message type', requestId);
      }
    } catch (error) {
      logger.error('Error handling WebSocket message:', error);
      this.sendError(ws, 'Invalid message format', undefined);
    }
  }

  private async handleGetTokenStatistics(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const { address } = payload;

      if (!address) {
        return this.sendError(ws, 'Contract address is required', requestId);
      }

      const tokenStats = await this.tokenStatisticsModel.findByAddress(address);

      if (!tokenStats) {
        return this.sendError(ws, 'Token statistics not found', requestId);
      }

      this.sendMessage(ws, {
        type: MessageType.GET_TOKEN_STATISTICS,
        payload: tokenStats,
        requestId,
      });
    } catch (error) {
      logger.error('Error getting token statistics by address:', error);
      this.sendError(ws, 'Failed to get token statistics', requestId);
    }
  }

  private async handleCreateTokenStatistics(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const validationRules = {
        contract_address: { required: true, type: 'string' },
        marketcap: { required: true, type: 'string' },
        is_trending: { required: true, type: 'number' },
        is_tracked: { required: true, type: 'number' },
        total_supply: { required: true, type: 'string' },
        current_price: { required: true, type: 'string' },
      };

      const validationErrors = validate(payload, validationRules);
      if (validationErrors.length > 0) {
        return this.sendError(ws, 'Validation failed', requestId, validationErrors);
      }

      await this.tokenStatisticsModel.createTokenStatistics(payload);

      this.sendMessage(ws, {
        type: MessageType.TOKEN_STATISTICS_CREATED,
        payload: {
          message: 'Token statistics accepted and queued for processing',
          status: 'queued'
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error queuing token statistics:', error);
      this.sendError(ws, 'Failed to create token statistics', requestId);
    }
  }

  private async handleBulkCreateTokenStatistics(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const { statistics } = payload;

      if (!Array.isArray(statistics) || statistics.length === 0) {
        return this.sendError(ws, 'Invalid input: Expected non-empty array of token statistics', requestId);
      }

      const validationRules = {
        contract_address: { required: true, type: 'string' },
        marketcap: { required: true, type: 'string' },
        is_trending: { required: true, type: 'number' },
        is_tracked: { required: true, type: 'number' },
        total_supply: { required: true, type: 'string' },
        current_price: { required: true, type: 'string' },
      };

      for (let i = 0; i < statistics.length; i++) {
        const validationErrors = validate(statistics[i], validationRules);
        if (validationErrors.length > 0) {
          return this.sendError(
            ws,
            `Invalid token statistics data at index ${i}: Missing required fields`,
            requestId,
            validationErrors
          );
        }
      }

      await this.tokenStatisticsModel.bulkCreateTokenStatistics(statistics);

      this.sendMessage(ws, {
        type: MessageType.TOKEN_STATISTICS_CREATED,
        payload: {
          message: 'Token statistics accepted and queued for processing',
          count: statistics.length,
          status: 'queued'
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error queuing token statistics:', error);
      this.sendError(ws, 'Failed to create token statistics', requestId);
    }
  }

  private async handleUpdateTokenStatistics(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const { address, data } = payload;

      if (!address) {
        return this.sendError(ws, 'Contract address is required', requestId);
      }

      if (!data || Object.keys(data).length === 0) {
        return this.sendError(ws, 'Update data is required', requestId);
      }

      // Prevent updating contract_address
      delete data.contract_address;

      const success = await this.tokenStatisticsModel.updateTokenStatistics(address, data);

      if (!success) {
        return this.sendError(ws, 'Token statistics not found', requestId);
      }

      this.sendMessage(ws, {
        type: MessageType.TOKEN_STATISTICS_UPDATED,
        payload: {
          message: 'Token statistics updated successfully',
          address
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error updating token statistics:', error);
      this.sendError(ws, 'Failed to update token statistics', requestId);
    }
  }

  private async handleGetQueueStatus(ws: WebSocket, requestId?: string) {
    try {
      const stats = this.tokenStatisticsModel.getQueueStats();

      this.sendMessage(ws, {
        type: MessageType.GET_QUEUE_STATUS,
        payload: {
          status: 'success',
          ...stats
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error getting token statistics queue status:', error);
      this.sendError(ws, 'Failed to get queue status', requestId);
    }
  }

  private sendMessage(ws: WebSocket, message: WebSocketMessage) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
    }
  }

  private sendError(ws: WebSocket, errorMessage: string, requestId?: string, details?: any) {
    this.sendMessage(ws, {
      type: MessageType.ERROR,
      payload: {
        message: errorMessage,
        details
      },
      requestId,
    });
  }
}

const router = Router();
let clickhouseClient: any;

(async function initializeRouterClient() {
  try {
    clickhouseClient = await setupClickhouse();
  } catch (error) {
    logger.error('Failed to initialize clickhouse client for HTTP router:', error);
  }
})();

// Get token statistics by contract address
router.get('/:address', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const tokenStatisticsModel = new TokenStatisticsModel(clickhouseClient);
    const { address } = req.params;
    
    if (!address) {
      return res.status(400).json({ error: 'Contract address is required' });
    }
    
    const tokenStats = await tokenStatisticsModel.findByAddress(address);
    
    if (!tokenStats) {
      return res.status(404).json({ error: 'Token statistics not found' });
    }
    
    return res.json(tokenStats);
  } catch (error) {
    logger.error('Error fetching token statistics:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// Create new token statistics
router.post('/create', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const tokenStatisticsModel = new TokenStatisticsModel(clickhouseClient);
    const tokenStatsData = req.body;

    const validationRules = {
      contract_address: { required: true, type: 'string' },
      marketcap: { required: true, type: 'string' },
      is_trending: { required: true, type: 'number' },
      is_tracked: { required: true, type: 'number' },
      total_supply: { required: true, type: 'string' },
      current_price: { required: true, type: 'string' },
    };

    const validationErrors = validate(tokenStatsData, validationRules);
    if (validationErrors.length > 0) {
      return res.status(400).json({ 
        error: 'Validation failed', 
        details: validationErrors 
      });
    }

    await tokenStatisticsModel.createTokenStatistics(tokenStatsData);

    return res.status(202).json({ 
      message: 'Token statistics accepted and queued for processing',
      status: 'queued'
    });
  } catch (error) {
    logger.error('Error creating token statistics:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// Bulk create token statistics
router.post('/bulk', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const tokenStatisticsModel = new TokenStatisticsModel(clickhouseClient);
    const { statistics } = req.body;

    if (!Array.isArray(statistics) || statistics.length === 0) {
      return res.status(400).json({ 
        error: 'Invalid input: Expected non-empty array of token statistics'
      });
    }

    const validationRules = {
      contract_address: { required: true, type: 'string' },
      marketcap: { required: true, type: 'string' },
      is_trending: { required: true, type: 'number' },
      is_tracked: { required: true, type: 'number' },
      total_supply: { required: true, type: 'string' },
      current_price: { required: true, type: 'string' },
    };

    for (let i = 0; i < statistics.length; i++) {
      const validationErrors = validate(statistics[i], validationRules);
      if (validationErrors.length > 0) {
        return res.status(400).json({
          error: `Invalid token statistics data at index ${i}: Missing required fields`,
          details: validationErrors
        });
      }
    }

    await tokenStatisticsModel.bulkCreateTokenStatistics(statistics);

    return res.status(202).json({
      message: 'Token statistics accepted and queued for processing',
      count: statistics.length,
      status: 'queued'
    });
  } catch (error) {
    logger.error('Error bulk creating token statistics:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// Get queue stats
router.get('/queue/stats', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const tokenStatisticsModel = new TokenStatisticsModel(clickhouseClient);
    const stats = tokenStatisticsModel.getQueueStats();
    
    return res.json(stats);
  } catch (error) {
    logger.error('Error fetching queue stats:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// Get all token statistics with pagination
router.get('/', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const tokenStatisticsModel = new TokenStatisticsModel(clickhouseClient);
    const page = parseInt(req.query.page as string) || 1;
    const limit = parseInt(req.query.limit as string) || 20;
    
    const result = await tokenStatisticsModel.findStatisticsByPage(page, limit);
    
    return res.json({
      data: result.statistics,
      pagination: {
        total: result.total,
        page,
        limit,
        pages: Math.ceil(result.total / limit)
      }
    });
  } catch (error) {
    logger.error('Error fetching token statistics:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// Update token statistics
router.put('/edit/:address', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const tokenStatisticsModel = new TokenStatisticsModel(clickhouseClient);
    const { address } = req.params;
    const tokenStatsData = req.body;

    if (!address || typeof address !== 'string') {
      return res.status(400).json({ error: 'Valid contract address is required' });
    }

    const validationRules = {
      marketcap: { required: false, type: 'string' },
      is_trending: { required: false, type: 'number' },
      is_tracked: { required: false, type: 'number' },
      total_supply: { required: false, type: 'string' },
      current_price: { required: false, type: 'string' },
    };

    const validationErrors = validate(tokenStatsData, validationRules);
    if (validationErrors.length > 0) {
      return res.status(400).json({
        error: 'Validation failed',
        details: validationErrors
      });
    }

    const success = await tokenStatisticsModel.updateTokenStatistics(address, tokenStatsData);

    if (!success) {
      return res.status(404).json({ error: 'Token statistics not found' });
    }

    return res.status(200).json({
      message: 'Token statistics updated successfully',
      contract_address: address
    });
  } catch (error) {
    logger.error('Error updating token statistics:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// Delete token statistics
router.delete('/delete/:address', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const tokenStatisticsModel = new TokenStatisticsModel(clickhouseClient);
    const { address } = req.params;

    if (!address || typeof address !== 'string') {
      return res.status(400).json({ error: 'Valid contract address is required' });
    }

    const success = await tokenStatisticsModel.deleteTokenStatistics(address);

    if (!success) {
      return res.status(404).json({ error: 'Token statistics not found' });
    }

    return res.status(200).json({
      message: 'Token statistics deleted successfully',
      contract_address: address
    });
  } catch (error) {
    logger.error('Error deleting token statistics:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});


export default router;