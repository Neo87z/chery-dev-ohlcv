import WebSocket from 'ws';
import { PairModel } from '../models/pair';
import { logger } from '../config/logger';
import { setupClickhouse } from '../config/db';

enum MessageType {
  GET_PAIR = 'GET_PAIR',
  GET_PAIRS_BY_TOKEN = 'GET_PAIRS_BY_TOKEN',
  CREATE_PAIR = 'CREATE_PAIR',
  BULK_CREATE_PAIRS = 'BULK_CREATE_PAIRS',
  UPDATE_PAIR = 'UPDATE_PAIR',
  DELETE_PAIR = 'DELETE_PAIR',
  GET_TRENDING_PAIRS = 'GET_TRENDING_PAIRS',
  GET_QUEUE_STATUS = 'GET_QUEUE_STATUS',
  PAIR_CREATED = 'PAIR_CREATED',
  PAIR_UPDATED = 'PAIR_UPDATED',
  ERROR = 'ERROR',
}

interface WebSocketMessage {
  type: MessageType;
  payload: any;
  requestId?: string;
}

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

export class PairWebSocketHandler {
  private clickhouseClient: any;
  private pairModel!: PairModel;

  constructor() {
    this.initialize();
  }

  private async initialize() {
    try {
      console.log('s')
      this.clickhouseClient = await setupClickhouse();
      this.pairModel = new PairModel(this.clickhouseClient);
    } catch (error) {
      logger.error('Failed to initialize PairWebSocketHandler:', error);
    }
  }

  public async handleMessage(ws: WebSocket, message: string) {
    try {
      const parsedMessage: WebSocketMessage = JSON.parse(message);
      const { type, payload, requestId } = parsedMessage;

      switch (type) {
        case MessageType.GET_PAIR:
          await this.handleGetPair(ws, payload, requestId);
          break;
        case MessageType.GET_PAIRS_BY_TOKEN:
          await this.handleGetPairsByToken(ws, payload, requestId);
          break;
        case MessageType.CREATE_PAIR:
          await this.handleCreatePair(ws, payload, requestId);
          break;
        case MessageType.BULK_CREATE_PAIRS:
          await this.handleBulkCreatePairs(ws, payload, requestId);
          break;
        case MessageType.UPDATE_PAIR:
          await this.handleUpdatePair(ws, payload, requestId);
          break;
        case MessageType.DELETE_PAIR:
          await this.handleDeletePair(ws, payload, requestId);
          break;
        case MessageType.GET_TRENDING_PAIRS:
          await this.handleGetTrendingPairs(ws, payload, requestId);
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

  private async handleGetPair(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const { pair_id } = payload;

      if (!pair_id) {
        return this.sendError(ws, 'Pair ID is required', requestId);
      }

      const pairInfo = await this.pairModel.findById(parseInt(pair_id, 10));

      if (!pairInfo) {
        return this.sendError(ws, 'Pair not found', requestId);
      }

      this.sendMessage(ws, {
        type: MessageType.GET_PAIR,
        payload: pairInfo,
        requestId,
      });
    } catch (error) {
      logger.error('Error getting pair by ID:', error);
      this.sendError(ws, 'Failed to get pair', requestId);
    }
  }

  private async handleGetPairsByToken(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const { token_id, page, limit } = payload;

      if (!token_id) {
        return this.sendError(ws, 'Token ID is required', requestId);
      }

      const pairs = await this.pairModel.findByTokenId(
        parseInt(token_id, 10), 
        { 
          page: page ? parseInt(page, 10) : 1, 
          limit: limit ? parseInt(limit, 10) : 20 
        }
      );

      this.sendMessage(ws, {
        type: MessageType.GET_PAIRS_BY_TOKEN,
        payload: {
          pairs,
          token_id: parseInt(token_id, 10),
          pagination: {
            page: page ? parseInt(page, 10) : 1,
            limit: limit ? parseInt(limit, 10) : 20
          }
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error getting pairs by token ID:', error);
      this.sendError(ws, 'Failed to get pairs by token', requestId);
    }
  }

  private async handleCreatePair(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const validationRules = {
        pair_id: { required: true, type: 'number' },
        token0_id: { required: true, type: 'number' },
        token1_id: { required: true, type: 'number' },
        exchange_id: { required: true, type: 'number' },
        liquidity: { required: true, type: 'string' },
        volume_24h: { required: true, type: 'string' },
        created_date: { required: true, type: 'string' },
      };

      const validationErrors = validate(payload, validationRules);
      if (validationErrors.length > 0) {
        return this.sendError(ws, 'Validation failed', requestId, validationErrors);
      }

      await this.pairModel.createPairInfo(payload);

      this.sendMessage(ws, {
        type: MessageType.PAIR_CREATED,
        payload: {
          message: 'Pair accepted and queued for processing',
          status: 'queued'
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error queuing pair:', error);
      this.sendError(ws, 'Failed to create pair', requestId);
    }
  }

  private async handleBulkCreatePairs(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const { pairs } = payload;

      if (!Array.isArray(pairs) || pairs.length === 0) {
        return this.sendError(ws, 'Invalid input: Expected non-empty array of pairs', requestId);
      }

      const validationRules = {
        pair_id: { required: true, type: 'number' },
        token0_id: { required: true, type: 'number' },
        token1_id: { required: true, type: 'number' },
        exchange_id: { required: true, type: 'number' },
        liquidity: { required: true, type: 'string' },
        volume_24h: { required: true, type: 'string' },
        created_date: { required: true, type: 'string' },
      };

      for (let i = 0; i < pairs.length; i++) {
        const validationErrors = validate(pairs[i], validationRules);
        if (validationErrors.length > 0) {
          return this.sendError(
            ws,
            `Invalid pair data at index ${i}: Missing required fields`,
            requestId,
            validationErrors
          );
        }
      }

      await this.pairModel.bulkCreatePairInfos(pairs);

      this.sendMessage(ws, {
        type: MessageType.PAIR_CREATED,
        payload: {
          message: 'Pairs accepted and queued for processing',
          count: pairs.length,
          status: 'queued'
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error queuing pairs:', error);
      this.sendError(ws, 'Failed to create pairs', requestId);
    }
  }

  private async handleUpdatePair(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const { pair_id, data } = payload;

      if (!pair_id) {
        return this.sendError(ws, 'Pair ID is required', requestId);
      }

      if (!data || Object.keys(data).length === 0) {
        return this.sendError(ws, 'Update data is required', requestId);
      }

      // Prevent updating pair_id
      delete data.pair_id;

      const success = await this.pairModel.updatePairInfo(parseInt(pair_id, 10), data);

      if (!success) {
        return this.sendError(ws, 'Pair not found', requestId);
      }

      this.sendMessage(ws, {
        type: MessageType.PAIR_UPDATED,
        payload: {
          message: 'Pair updated successfully',
          pair_id: parseInt(pair_id, 10)
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error updating pair:', error);
      this.sendError(ws, 'Failed to update pair', requestId);
    }
  }

  private async handleDeletePair(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const { pair_id } = payload;

      if (!pair_id) {
        return this.sendError(ws, 'Pair ID is required', requestId);
      }

      const success = await this.pairModel.deletePairInfo(parseInt(pair_id, 10));

      if (!success) {
        return this.sendError(ws, 'Pair not found', requestId);
      }

      this.sendMessage(ws, {
        type: MessageType.DELETE_PAIR,
        payload: {
          message: 'Pair deleted successfully',
          pair_id: parseInt(pair_id, 10)
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error deleting pair:', error);
      this.sendError(ws, 'Failed to delete pair', requestId);
    }
  }

  private async handleGetTrendingPairs(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const page = payload?.page ? parseInt(payload.page, 10) : 1;
      const limit = payload?.limit ? parseInt(payload.limit, 10) : 10;

      const result = await this.pairModel.findTrendingPairs(page, limit);

      this.sendMessage(ws, {
        type: MessageType.GET_TRENDING_PAIRS,
        payload: {
          pairs: result.pairs,
          pagination: {
            total: result.total,
            page,
            limit,
            pages: Math.ceil(result.total / limit)
          }
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error getting trending pairs:', error);
      this.sendError(ws, 'Failed to get trending pairs', requestId);
    }
  }

  private async handleGetQueueStatus(ws: WebSocket, requestId?: string) {
    try {
      const stats = this.pairModel.getQueueStats();

      this.sendMessage(ws, {
        type: MessageType.GET_QUEUE_STATUS,
        payload: {
          status: 'success',
          ...stats
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error getting pair queue status:', error);
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

