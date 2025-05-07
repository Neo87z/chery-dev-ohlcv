import { Router } from 'express';
import WebSocket from 'ws';
import { ClickHouseClient } from '@clickhouse/client';
import { TokenInfoModel } from '../models/token';
import { logger } from '../config/logger';
import { setupClickhouse } from '../config/db';

enum MessageType {
  GET_TOKEN_INFO = 'GET_TOKEN_INFO',
  CREATE_TOKEN_INFO = 'CREATE_TOKEN_INFO',
  BULK_CREATE_TOKEN_INFOS = 'BULK_CREATE_TOKEN_INFOS',
  UPDATE_TOKEN_INFO = 'UPDATE_TOKEN_INFO',
  GET_QUEUE_STATUS = 'GET_QUEUE_STATUS',
  TOKEN_INFO_CREATED = 'TOKEN_INFO_CREATED',
  TOKEN_INFO_UPDATED = 'TOKEN_INFO_UPDATED',
  ERROR = 'ERROR',
}

interface WebSocketMessage {
  type: MessageType;
  payload: any;
  requestId?: string;
}

(async function initializeTokenInfoQueue() {
  try {
    const client = await setupClickhouse();
    TokenInfoModel.initializeQueue(client);
    logger.info('Token info queue initialized successfully');
  } catch (error) {
    logger.error('Failed to initialize token info queue:', error);
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

export class TokenInfoWebSocketHandler {
  private clickhouseClient: any;
  private tokenInfoModel!: TokenInfoModel;

  constructor() {
    this.initialize();
  }

  private async initialize() {
    try {
      this.clickhouseClient = await setupClickhouse();
      this.tokenInfoModel = new TokenInfoModel(this.clickhouseClient);
    } catch (error) {
      logger.error('Failed to initialize TokenInfoWebSocketHandler:', error);
    }
  }

  public async handleMessage(ws: WebSocket, message: string) {
    try {
      const parsedMessage: WebSocketMessage = JSON.parse(message);
      const { type, payload, requestId } = parsedMessage;

      switch (type) {
        case MessageType.GET_TOKEN_INFO:
          await this.handleGetTokenInfo(ws, payload, requestId);
          break;
        case MessageType.CREATE_TOKEN_INFO:
          await this.handleCreateTokenInfo(ws, payload, requestId);
          break;
        case MessageType.BULK_CREATE_TOKEN_INFOS:
          await this.handleBulkCreateTokenInfos(ws, payload, requestId);
          break;
        case MessageType.UPDATE_TOKEN_INFO:
          await this.handleUpdateTokenInfo(ws, payload, requestId);
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

  private async handleGetTokenInfo(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const { address } = payload;

      if (!address) {
        return this.sendError(ws, 'Contract address is required', requestId);
      }

      const tokenInfo = await this.tokenInfoModel.findByAddress(address);

      if (!tokenInfo) {
        return this.sendError(ws, 'Token info not found', requestId);
      }

      this.sendMessage(ws, {
        type: MessageType.GET_TOKEN_INFO,
        payload: tokenInfo,
        requestId,
      });
    } catch (error) {
      logger.error('Error getting token info by address:', error);
      this.sendError(ws, 'Failed to get token info', requestId);
    }
  }

  private async handleCreateTokenInfo(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const validationRules = {
        contract_address: { required: true, type: 'string' },
        create_date: { required: true, type: 'string' },
        makers: { required: true, type: 'object' },
        volume: { required: true, type: 'string' },
        image_link: { required: true, type: 'string' },
        holders: { required: true, type: 'number' },
      };

      const validationErrors = validate(payload, validationRules);
      if (validationErrors.length > 0) {
        return this.sendError(ws, 'Validation failed', requestId, validationErrors);
      }

      await this.tokenInfoModel.createTokenInfo(payload);

      this.sendMessage(ws, {
        type: MessageType.TOKEN_INFO_CREATED,
        payload: {
          message: 'Token info accepted and queued for processing',
          status: 'queued'
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error queuing token info:', error);
      this.sendError(ws, 'Failed to create token info', requestId);
    }
  }

  private async handleBulkCreateTokenInfos(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const { tokens } = payload;

      if (!Array.isArray(tokens) || tokens.length === 0) {
        return this.sendError(ws, 'Invalid input: Expected non-empty array of token infos', requestId);
      }

      const validationRules = {
        contract_address: { required: true, type: 'string' },
        create_date: { required: true, type: 'string' },
        makers: { required: true, type: 'object' },
        volume: { required: true, type: 'string' },
        image_link: { required: true, type: 'string' },
        holders: { required: true, type: 'number' },
      };

      for (let i = 0; i < tokens.length; i++) {
        const validationErrors = validate(tokens[i], validationRules);
        if (validationErrors.length > 0) {
          return this.sendError(
            ws,
            `Invalid token info data at index ${i}: Missing required fields`,
            requestId,
            validationErrors
          );
        }
      }

      await this.tokenInfoModel.bulkCreateTokenInfos(tokens);

      this.sendMessage(ws, {
        type: MessageType.TOKEN_INFO_CREATED,
        payload: {
          message: 'Token infos accepted and queued for processing',
          count: tokens.length,
          status: 'queued'
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error queuing token infos:', error);
      this.sendError(ws, 'Failed to create token infos', requestId);
    }
  }

  private async handleUpdateTokenInfo(ws: WebSocket, payload: any, requestId?: string) {
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

      const success = await this.tokenInfoModel.updateTokenInfo(address, data);

      if (!success) {
        return this.sendError(ws, 'Token info not found', requestId);
      }

      this.sendMessage(ws, {
        type: MessageType.TOKEN_INFO_UPDATED,
        payload: {
          message: 'Token info updated successfully',
          address
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error updating token info:', error);
      this.sendError(ws, 'Failed to update token info', requestId);
    }
  }

  private async handleGetQueueStatus(ws: WebSocket, requestId?: string) {
    try {
      const stats = this.tokenInfoModel.getQueueStats();

      this.sendMessage(ws, {
        type: MessageType.GET_QUEUE_STATUS,
        payload: {
          status: 'success',
          ...stats
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error getting token info queue status:', error);
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

// Get token info by contract address
router.get('/:address', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const tokenInfoModel = new TokenInfoModel(clickhouseClient);
    const { address } = req.params;
    
    if (!address) {
      return res.status(400).json({ error: 'Contract address is required' });
    }
    
    const tokenInfo = await tokenInfoModel.findByAddress(address);
    
    if (!tokenInfo) {
      return res.status(404).json({ error: 'Token info not found' });
    }
    
    return res.json(tokenInfo);
  } catch (error) {
    logger.error('Error fetching token info:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// Create new token info
router.post('/create', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const tokenInfoModel = new TokenInfoModel(clickhouseClient);
    const tokenInfoData = req.body;

    const validationRules = {
      contract_address: { required: true, type: 'string' },
      create_date: { required: true, type: 'string' },
      makers: { required: true, type: 'object' },
      volume: { required: true, type: 'string' },
      image_link: { required: true, type: 'string' },
      holders: { required: true, type: 'number' },
    };

    const validationErrors = validate(tokenInfoData, validationRules);
    if (validationErrors.length > 0) {
      return res.status(400).json({ 
        error: 'Validation failed', 
        details: validationErrors 
      });
    }

    await tokenInfoModel.createTokenInfo(tokenInfoData);

    return res.status(202).json({ 
      message: 'Token info accepted and queued for processing',
      status: 'queued'
    });
  } catch (error) {
    logger.error('Error creating token info:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// Bulk create token infos
router.post('/bulk', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const tokenInfoModel = new TokenInfoModel(clickhouseClient);
    const { tokens } = req.body;

    if (!Array.isArray(tokens) || tokens.length === 0) {
      return res.status(400).json({ 
        error: 'Invalid input: Expected non-empty array of token infos'
      });
    }

    const validationRules = {
      contract_address: { required: true, type: 'string' },
      create_date: { required: true, type: 'string' },
      makers: { required: true, type: 'object' },
      volume: { required: true, type: 'string' },
      image_link: { required: true, type: 'string' },
      holders: { required: true, type: 'number' },
    };

    for (let i = 0; i < tokens.length; i++) {
      const validationErrors = validate(tokens[i], validationRules);
      if (validationErrors.length > 0) {
        return res.status(400).json({
          error: `Invalid token info data at index ${i}: Missing required fields`,
          details: validationErrors
        });
      }
    }

    await tokenInfoModel.bulkCreateTokenInfos(tokens);

    return res.status(202).json({
      message: 'Token infos accepted and queued for processing',
      count: tokens.length,
      status: 'queued'
    });
  } catch (error) {
    logger.error('Error bulk creating token infos:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// Get queue stats
router.get('/queue/stats', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const tokenInfoModel = new TokenInfoModel(clickhouseClient);
    const stats = tokenInfoModel.getQueueStats();
    
    return res.json(stats);
  } catch (error) {
    logger.error('Error fetching queue stats:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// Get all token infos with pagination
router.get('/', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const tokenInfoModel = new TokenInfoModel(clickhouseClient);
    const page = parseInt(req.query.page as string) || 1;
    const limit = parseInt(req.query.limit as string) || 20;
    
    const result = await tokenInfoModel.findTokensByPage(page, limit);
    
    return res.json({
      data: result.tokens,
      pagination: {
        total: result.total,
        page,
        limit,
        pages: Math.ceil(result.total / limit)
      }
    });
  } catch (error) {
    logger.error('Error fetching token infos:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// Update token info
router.put('/edit/:address', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const tokenInfoModel = new TokenInfoModel(clickhouseClient);
    const { address } = req.params;
    const tokenInfoData = req.body;

    if (!address || typeof address !== 'string') {
      return res.status(400).json({ error: 'Valid contract address is required' });
    }

    const validationRules = {
      create_date: { required: false, type: 'string' },
      makers: { required: false, type: 'object' },
      volume: { required: false, type: 'string' },
      image_link: { required: false, type: 'string' },
      holders: { required: false, type: 'number' },
    };

    const validationErrors = validate(tokenInfoData, validationRules);
    if (validationErrors.length > 0) {
      return res.status(400).json({
        error: 'Validation failed',
        details: validationErrors
      });
    }

    const success = await tokenInfoModel.updateTokenInfo(address, tokenInfoData);

    if (!success) {
      return res.status(404).json({ error: 'Token info not found' });
    }

    return res.status(200).json({
      message: 'Token info updated successfully',
      contract_address: address
    });
  } catch (error) {
    logger.error('Error updating token info:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// Delete token info
router.delete('/delete/:address', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const tokenInfoModel = new TokenInfoModel(clickhouseClient);
    const { address } = req.params;

    if (!address || typeof address !== 'string') {
      return res.status(400).json({ error: 'Valid contract address is required' });
    }

    const success = await tokenInfoModel.deleteTokenInfo(address);

    if (!success) {
      return res.status(404).json({ error: 'Token info not found' });
    }

    return res.status(200).json({
      message: 'Token info deleted successfully',
      contract_address: address
    });
  } catch (error) {
    logger.error('Error deleting token info:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// Get trending tokens based on volume
router.get('/trending', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const page = parseInt(req.query.page as string) || 1;
    const limit = parseInt(req.query.limit as string) || 10;
    
    // Query to get trending tokens based on volume
    const query = `
      SELECT * FROM token_info
      ORDER BY CAST(volume AS Float64) DESC
      LIMIT ${limit} OFFSET ${(page - 1) * limit}
    `;
    
    const result = await clickhouseClient.query({
      query,
      format: 'JSONEachRow'
    });
    
    const tokens = await result.json();
    
    return res.json({
      data: tokens,
      pagination: {
        page,
        limit
      }
    });
  } catch (error) {
    logger.error('Error fetching trending tokens:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;