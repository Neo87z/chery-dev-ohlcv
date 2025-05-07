import { Router } from 'express';
import WebSocket from 'ws';
import { ClickHouseClient } from '@clickhouse/client';
import { SocialInfoModel } from '../models/social';
import { logger } from '../config/logger';
import { setupClickhouse } from '../config/db';

enum MessageType {
  GET_SOCIAL_INFO = 'GET_SOCIAL_INFO',
  CREATE_SOCIAL_INFO = 'CREATE_SOCIAL_INFO',
  BULK_CREATE_SOCIAL_INFOS = 'BULK_CREATE_SOCIAL_INFOS',
  UPDATE_SOCIAL_INFO = 'UPDATE_SOCIAL_INFO',
  GET_QUEUE_STATUS = 'GET_QUEUE_STATUS',
  SOCIAL_INFO_CREATED = 'SOCIAL_INFO_CREATED',
  SOCIAL_INFO_UPDATED = 'SOCIAL_INFO_UPDATED',
  ERROR = 'ERROR',
}

interface WebSocketMessage {
  type: MessageType;
  payload: any;
  requestId?: string;
}

(async function initializeSocialInfoQueue() {
  try {
    const client = await setupClickhouse();
    SocialInfoModel.initializeQueue(client);
    logger.info('Social info queue initialized successfully');
  } catch (error) {
    logger.error('Failed to initialize social info queue:', error);
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

export class SocialInfoWebSocketHandler {
  private clickhouseClient: any;
  private socialInfoModel!: SocialInfoModel;

  constructor() {
    this.initialize();
  }

  private async initialize() {
    try {
      this.clickhouseClient = await setupClickhouse();
      this.socialInfoModel = new SocialInfoModel(this.clickhouseClient);
    } catch (error) {
      logger.error('Failed to initialize SocialInfoWebSocketHandler:', error);
    }
  }

  public async handleMessage(ws: WebSocket, message: string) {
    try {
      const parsedMessage: WebSocketMessage = JSON.parse(message);
      const { type, payload, requestId } = parsedMessage;

      switch (type) {
        case MessageType.GET_SOCIAL_INFO:
          await this.handleGetSocialInfo(ws, payload, requestId);
          break;
        case MessageType.CREATE_SOCIAL_INFO:
          await this.handleCreateSocialInfo(ws, payload, requestId);
          break;
        case MessageType.BULK_CREATE_SOCIAL_INFOS:
          await this.handleBulkCreateSocialInfos(ws, payload, requestId);
          break;
        case MessageType.UPDATE_SOCIAL_INFO:
          await this.handleUpdateSocialInfo(ws, payload, requestId);
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

  private async handleGetSocialInfo(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const { address } = payload;

      if (!address) {
        return this.sendError(ws, 'Contract address is required', requestId);
      }

      const socialInfo = await this.socialInfoModel.findByAddress(address);

      if (!socialInfo) {
        return this.sendError(ws, 'Social info not found', requestId);
      }

      this.sendMessage(ws, {
        type: MessageType.GET_SOCIAL_INFO,
        payload: socialInfo,
        requestId,
      });
    } catch (error) {
      logger.error('Error getting social info by address:', error);
      this.sendError(ws, 'Failed to get social info', requestId);
    }
  }

  private async handleCreateSocialInfo(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const validationRules = {
        contract_address: { required: true, type: 'number' },
        website: { required: false, type: 'string' },
        telegram: { required: false, type: 'string' },
        other_social: { required: false, type: 'object' },
        explorer: { required: false, type: 'string' },
        github: { required: false, type: 'string' },
      };

      const validationErrors = validate(payload, validationRules);
      if (validationErrors.length > 0) {
        return this.sendError(ws, 'Validation failed', requestId, validationErrors);
      }

      await this.socialInfoModel.createSocialInfo(payload);

      this.sendMessage(ws, {
        type: MessageType.SOCIAL_INFO_CREATED,
        payload: {
          message: 'Social info accepted and queued for processing',
          status: 'queued'
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error queuing social info:', error);
      this.sendError(ws, 'Failed to create social info', requestId);
    }
  }

  private async handleBulkCreateSocialInfos(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const { socials } = payload;

      if (!Array.isArray(socials) || socials.length === 0) {
        return this.sendError(ws, 'Invalid input: Expected non-empty array of social infos', requestId);
      }

      const validationRules = {
        contract_address: { required: true, type: 'number' },
        website: { required: false, type: 'string' },
        telegram: { required: false, type: 'string' },
        other_social: { required: false, type: 'object' },
        explorer: { required: false, type: 'string' },
        github: { required: false, type: 'string' },
      };

      for (let i = 0; i < socials.length; i++) {
        const validationErrors = validate(socials[i], validationRules);
        if (validationErrors.length > 0) {
          return this.sendError(
            ws,
            `Invalid social info data at index ${i}: Missing required fields`,
            requestId,
            validationErrors
          );
        }
      }

      await this.socialInfoModel.bulkCreateSocialInfos(socials);

      this.sendMessage(ws, {
        type: MessageType.SOCIAL_INFO_CREATED,
        payload: {
          message: 'Social infos accepted and queued for processing',
          count: socials.length,
          status: 'queued'
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error queuing social infos:', error);
      this.sendError(ws, 'Failed to create social infos', requestId);
    }
  }

  private async handleUpdateSocialInfo(ws: WebSocket, payload: any, requestId?: string) {
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

      const success = await this.socialInfoModel.updateSocialInfo(address, data);

      if (!success) {
        return this.sendError(ws, 'Social info not found', requestId);
      }

      this.sendMessage(ws, {
        type: MessageType.SOCIAL_INFO_UPDATED,
        payload: {
          message: 'Social info updated successfully',
          address
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error updating social info:', error);
      this.sendError(ws, 'Failed to update social info', requestId);
    }
  }

  private async handleGetQueueStatus(ws: WebSocket, requestId?: string) {
    try {
      const stats = this.socialInfoModel.getQueueStats();

      this.sendMessage(ws, {
        type: MessageType.GET_QUEUE_STATUS,
        payload: {
          status: 'success',
          ...stats
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error getting social info queue status:', error);
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


router.get('/:address', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const socialInfoModel = new SocialInfoModel(clickhouseClient);
    const { address } = req.params;
    
    if (!address) {
      return res.status(400).json({ error: 'Contract address is required' });
    }
    
    const socialInfo = await socialInfoModel.findByAddress(address);
    
    if (!socialInfo) {
      return res.status(404).json({ error: 'Social info not found' });
    }
    
    return res.json(socialInfo);
  } catch (error) {
    logger.error('Error fetching social info:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});


router.post('/create', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const socialInfoModel = new SocialInfoModel(clickhouseClient);
    const socialInfoData = req.body;

    const validationRules = {
      contract_address: { required: true, type: 'number' },
      website: { required: false, type: 'string' },
      telegram: { required: false, type: 'string' },
      other_social: { required: false, type: 'object' },
      explorer: { required: false, type: 'string' },
      github: { required: false, type: 'string' },
    };

    const validationErrors = validate(socialInfoData, validationRules);
    if (validationErrors.length > 0) {
      return res.status(400).json({ 
        error: 'Validation failed', 
        details: validationErrors 
      });
    }

    await socialInfoModel.createSocialInfo(socialInfoData);

    return res.status(202).json({ 
      message: 'Social info accepted and queued for processing',
      status: 'queued'
    });
  } catch (error) {
    logger.error('Error creating social info:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});


router.post('/bulk', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const socialInfoModel = new SocialInfoModel(clickhouseClient);
    const { socials } = req.body;

    if (!Array.isArray(socials) || socials.length === 0) {
      return res.status(400).json({ 
        error: 'Invalid input: Expected non-empty array of social infos'
      });
    }

    const validationRules = {
      contract_address: { required: true, type: 'number' },
      website: { required: false, type: 'string' },
      telegram: { required: false, type: 'string' },
      other_social: { required: false, type: 'object' },
      explorer: { required: false, type: 'string' },
      github: { required: false, type: 'string' },
    };

    for (let i = 0; i < socials.length; i++) {
      const validationErrors = validate(socials[i], validationRules);
      if (validationErrors.length > 0) {
        return res.status(400).json({
          error: `Invalid social info data at index ${i}: Missing required fields`,
          details: validationErrors
        });
      }
    }

    await socialInfoModel.bulkCreateSocialInfos(socials);

    return res.status(202).json({
      message: 'Social infos accepted and queued for processing',
      count: socials.length,
      status: 'queued'
    });
  } catch (error) {
    logger.error('Error bulk creating social infos:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});


router.get('/queue/stats', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const socialInfoModel = new SocialInfoModel(clickhouseClient);
    const stats = socialInfoModel.getQueueStats();
    
    return res.json(stats);
  } catch (error) {
    logger.error('Error fetching queue stats:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});


router.get('/', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const socialInfoModel = new SocialInfoModel(clickhouseClient);
    const page = parseInt(req.query.page as string) || 1;
    const limit = parseInt(req.query.limit as string) || 20;
    
    const result = await socialInfoModel.findByPage(page, limit);
    
    return res.json({
      data: result.socials,
      pagination: {
        total: result.total,
        page,
        limit,
        pages: Math.ceil(result.total / limit)
      }
    });
  } catch (error) {
    logger.error('Error fetching social infos:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});


router.put('/edit/:address', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const socialInfoModel = new SocialInfoModel(clickhouseClient);
    const { address } = req.params;
    const socialInfoData = req.body;

    if (!address) {
      return res.status(400).json({ error: 'Valid contract address is required' });
    }

    const validationRules = {
      website: { required: false, type: 'string' },
      telegram: { required: false, type: 'string' },
      other_social: { required: false, type: 'object' },
      explorer: { required: false, type: 'string' },
      github: { required: false, type: 'string' },
    };

    const validationErrors = validate(socialInfoData, validationRules);
    if (validationErrors.length > 0) {
      return res.status(400).json({
        error: 'Validation failed',
        details: validationErrors
      });
    }

    const success = await socialInfoModel.updateSocialInfo(address, socialInfoData);

    if (!success) {
      return res.status(404).json({ error: 'Social info not found' });
    }

    return res.status(200).json({
      message: 'Social info updated successfully',
      contract_address: address
    });
  } catch (error) {
    logger.error('Error updating social info:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});


router.delete('/delete/:address', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const socialInfoModel = new SocialInfoModel(clickhouseClient);
    const { address } = req.params;

    if (!address) {
      return res.status(400).json({ error: 'Valid contract address is required' });
    }

    const success = await socialInfoModel.deleteTokenInfo(address);

    if (!success) {
      return res.status(404).json({ error: 'Social info not found' });
    }

    return res.status(200).json({
      message: 'Social info deleted successfully',
      contract_address: address
    });
  } catch (error) {
    logger.error('Error deleting social info:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;