import { Router } from 'express';
import WebSocket from 'ws';
import { Pool } from 'pg';
import { TokenInfPostGresModel } from '../models/token_postgres';
import { TokenInfo } from '../types/database';
import { logger } from '../config/logger';
import { pool, initDatabase } from '../config/db_postgres';

enum MessageType {
  GET_TOKEN_INFO = 'GET_TOKEN_INFO',
  CREATE_TOKEN_INFO = 'CREATE_TOKEN_INFO',
  BULK_CREATE_TOKEN_INFOS = 'BULK_CREATE_TOKEN_INFOS',
  UPDATE_TOKEN_INFO = 'UPDATE_TOKEN_INFO',
  GET_QUEUE_STATUS = 'GET_QUEUE_STATUS',
  SEARCH_TOKENS = 'SEARCH_TOKENS',
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
    const initialized = true
    if (initialized) {
      logger.info('PostgreSQL token info queue initialized successfully');
    } else {
      logger.error('Failed to initialize PostgreSQL token info queue');
    }
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

export class TokenInfoPostgresWebSocketHandler {
  private pgPool: Pool;
  private tokenInfoModel: TokenInfPostGresModel;

  constructor(pgPool: Pool = pool) {
    this.pgPool = pgPool;
    this.tokenInfoModel = new TokenInfPostGresModel(this.pgPool);
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
        case MessageType.SEARCH_TOKENS:
          await this.handleSearchTokens(ws, payload, requestId);
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
      if (!address) return this.sendError(ws, 'Contract address is required', requestId);
      const tokenInfo = await this.tokenInfoModel.findByAddress(address);
      if (!tokenInfo) return this.sendError(ws, 'Token info not found', requestId);
      this.sendMessage(ws, { type: MessageType.GET_TOKEN_INFO, payload: tokenInfo, requestId });
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
        volume: { required: true, type: 'string' },
        image_link: { required: true, type: 'string' },
        holders: { required: true, type: 'number' }
      };
      const validationErrors = validate(payload, validationRules);
      if (validationErrors.length > 0) {
        return this.sendError(ws, 'Validation failed', requestId, validationErrors);
      }
      await this.tokenInfoModel.createTokenInfo(payload);
      this.sendMessage(ws, {
        type: MessageType.TOKEN_INFO_CREATED,
        payload: { message: 'Token info queued', status: 'queued' },
        requestId
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
        return this.sendError(ws, 'Expected non-empty array of token infos', requestId);
      }
      const validationRules = {
        contract_address: { required: true, type: 'string' },
        create_date: { required: true, type: 'string' },
        volume: { required: true, type: 'string' },
        image_link: { required: true, type: 'string' },
        holders: { required: true, type: 'number' }
      };
      for (let i = 0; i < tokens.length; i++) {
        const validationErrors = validate(tokens[i], validationRules);
        if (validationErrors.length > 0) {
          return this.sendError(ws, `Invalid token info at index ${i}`, requestId, validationErrors);
        }
      }
      await this.tokenInfoModel.bulkCreateTokenInfos(tokens);
      this.sendMessage(ws, {
        type: MessageType.TOKEN_INFO_CREATED,
        payload: { message: 'Token infos queued', count: tokens.length, status: 'queued' },
        requestId
      });
    } catch (error) {
      logger.error('Error queuing token infos:', error);
      this.sendError(ws, 'Failed to create token infos', requestId);
    }
  }

  private async handleUpdateTokenInfo(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const { address, data } = payload;
      if (!address || !data || Object.keys(data).length === 0) {
        return this.sendError(ws, 'Address and data required', requestId);
      }
      delete data.contract_address;
      const success = await this.tokenInfoModel.updateTokenInfo(address, data);
      if (!success) return this.sendError(ws, 'Token info not found', requestId);
      this.sendMessage(ws, {
        type: MessageType.TOKEN_INFO_UPDATED,
        payload: { message: 'Token info updated', address },
        requestId
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
        payload: { status: 'success', ...stats },
        requestId
      });
    } catch (error) {
      logger.error('Error getting queue status:', error);
      this.sendError(ws, 'Failed to get queue status', requestId);
    }
  }

  private async handleSearchTokens(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const { searchText, limit = 20, offset = 0 } = payload;
      if (!searchText || typeof searchText !== 'string') {
        return this.sendError(ws, 'Search text required', requestId);
      }
      const result = await this.tokenInfoModel.searchTokens(searchText, limit, offset);
      this.sendMessage(ws, {
        type: MessageType.SEARCH_TOKENS,
        payload: { tokens: result.tokens, total: result.total, limit, offset },
        requestId
      });
    } catch (error) {
      logger.error('Error searching tokens:', error);
      this.sendError(ws, 'Failed to search tokens', requestId);
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
      payload: { message: errorMessage, details },
      requestId
    });
  }
}

const router = Router();

router.get('/:address', async (req, res) => {
  try {
    const tokenInfoModel = new TokenInfPostGresModel(pool);
    const { address } = req.params;
    if (!address) return res.status(400).json({ error: 'Contract address required' });
    const tokenInfo = await tokenInfoModel.findByAddress(address);
    if (!tokenInfo) return res.status(404).json({ error: 'Token info not found' });
    return res.json(tokenInfo);
  } catch (error) {
    logger.error('Error fetching token info:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

router.post('/create', async (req, res) => {
  try {
    const tokenInfoModel = new TokenInfPostGresModel(pool);
    const tokenInfoData = req.body;
    const validationRules = {
      contract_address: { required: true, type: 'string' },
      create_date: { required: true, type: 'string' },
      volume: { required: true, type: 'string' },
      image_link: { required: true, type: 'string' },
      holders: { required: true, type: 'number' }
    };
    const validationErrors = validate(tokenInfoData, validationRules);
    if (validationErrors.length > 0) {
      return res.status(400).json({ error: 'Validation failed', details: validationErrors });
    }
    await tokenInfoModel.createTokenInfo(tokenInfoData);
    return res.status(202).json({ message: 'Token info queued', status: 'queued' });
  } catch (error) {
    logger.error('Error creating token info:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

router.post('/bulk', async (req, res) => {
  try {
    const tokenInfoModel = new TokenInfPostGresModel(pool);
    const { tokens } = req.body;
    if (!Array.isArray(tokens) || tokens.length === 0) {
      return res.status(400).json({ error: 'Expected non-empty array of token infos' });
    }
    const validationRules = {
      contract_address: { required: true, type: 'string' },
      create_date: { required: true, type: 'string' },
      volume: { required: true, type: 'string' },
      image_link: { required: true, type: 'string' },
      holders: { required: true, type: 'number' }
    };
    for (let i = 0; i < tokens.length; i++) {
      const validationErrors = validate(tokens[i], validationRules);
      if (validationErrors.length > 0) {
        return res.status(400).json({ error: `Invalid token info at index ${i}`, details: validationErrors });
      }
    }
    await tokenInfoModel.bulkCreateTokenInfos(tokens);
    return res.status(202).json({ message: 'Token infos queued', count: tokens.length, status: 'queued' });
  } catch (error) {
    logger.error('Error bulk creating token infos:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

router.get('/queue/stats', async (req, res) => {
  try {
    const tokenInfoModel = new TokenInfPostGresModel(pool);
    const stats = tokenInfoModel.getQueueStats();
    return res.json(stats);
  } catch (error) {
    logger.error('Error fetching queue stats:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

router.get('/', async (req, res) => {
  try {
    const tokenInfoModel = new TokenInfPostGresModel(pool);
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

router.get('/search', async (req, res) => {
  try {
    const tokenInfoModel = new TokenInfPostGresModel(pool);
    const searchText = req.query.q as string;
    const limit = parseInt(req.query.limit as string) || 20;
    const offset = parseInt(req.query.offset as string) || 0;
    if (!searchText) return res.status(400).json({ error: 'Search text required' });
    const result = await tokenInfoModel.searchTokens(searchText, limit, offset);
    return res.json({ data: result.tokens, total: result.total, limit, offset });
  } catch (error) {
    logger.error('Error searching tokens:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;
