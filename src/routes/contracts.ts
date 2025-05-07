import { Router } from 'express';
import WebSocket from 'ws';
import { ClickHouseClient } from '@clickhouse/client';
import { ContractModel } from '../models/contract';
import { logger } from '../config/logger';
import { setupClickhouse } from '../config/db';


enum MessageType {
  GET_CONTRACT = 'GET_CONTRACT',
  CREATE_CONTRACT = 'CREATE_CONTRACT',
  BULK_CREATE_CONTRACTS = 'BULK_CREATE_CONTRACTS',
  GET_QUEUE_STATUS = 'GET_QUEUE_STATUS',
  CONTRACT_CREATED = 'CONTRACT_CREATED',
  ERROR = 'ERROR',
}


interface WebSocketMessage {
  type: MessageType;
  payload: any;
  requestId?: string; 
}


(async function initializeContractQueue() {
  try {
    const client = await setupClickhouse();
    ContractModel.initializeQueue(client);
    logger.info('Contract queue initialized successfully');
  } catch (error) {
    logger.error('Failed to initialize contract queue:', error);
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


export class ContractsWebSocketHandler {
  private clickhouseClient: any;
  private contractModel!: ContractModel;

  constructor() {
    this.initialize();
  }

  private async initialize() {
    try {
      this.clickhouseClient = await setupClickhouse();
      this.contractModel = new ContractModel(this.clickhouseClient);
    } catch (error) {
      logger.error('Failed to initialize ContractsWebSocketHandler:', error);
    }
  }

  
  public async handleMessage(ws: WebSocket, message: string) {
    try {
      const parsedMessage: WebSocketMessage = JSON.parse(message);
      const { type, payload, requestId } = parsedMessage;

      switch (type) {
        case MessageType.GET_CONTRACT:
          await this.handleGetContract(ws, payload, requestId);
          break;
        case MessageType.CREATE_CONTRACT:
          await this.handleCreateContract(ws, payload, requestId);
          break;
        case MessageType.BULK_CREATE_CONTRACTS:
          await this.handleBulkCreateContracts(ws, payload, requestId);
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

  private async handleGetContract(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const { address } = payload;

      if (!address) {
        return this.sendError(ws, 'Contract address is required', requestId);
      }

      const contract = await this.contractModel.findByAddress(address);

      if (!contract) {
        return this.sendError(ws, 'Contract not found', requestId);
      }

      this.sendMessage(ws, {
        type: MessageType.GET_CONTRACT,
        payload: contract,
        requestId,
      });
    } catch (error) {
      logger.error('Error getting contract by address:', error);
      this.sendError(ws, 'Failed to get contract', requestId);
    }
  }

  private async handleCreateContract(ws: WebSocket, payload: any, requestId?: string) {
    try {
      
      const validationRules = {
        contract_address: { required: true, type: 'string' },
        chain: { required: true, type: 'string' },
        symbol: { required: true, type: 'string' },
        token_name: { required: true, type: 'string' },
      };

      const validationErrors = validate(payload, validationRules);
      if (validationErrors.length > 0) {
        return this.sendError(ws, 'Validation failed', requestId, validationErrors);
      }

    
      await this.contractModel.createContract(payload);

      
      this.sendMessage(ws, {
        type: MessageType.CONTRACT_CREATED,
        payload: {
          message: 'Contract accepted and queued for processing',
          status: 'queued'
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error queuing contract:', error);
      this.sendError(ws, 'Failed to create contract', requestId);
    }
  }

  private async handleBulkCreateContracts(ws: WebSocket, payload: any, requestId?: string) {
    try {
      const { contracts } = payload;

      if (!Array.isArray(contracts) || contracts.length === 0) {
        return this.sendError(ws, 'Invalid input: Expected non-empty array of contracts', requestId);
      }


      const validationRules = {
        contract_address: { required: true, type: 'string' },
        chain: { required: true, type: 'string' },
        symbol: { required: true, type: 'string' },
        token_name: { required: true, type: 'string' },
      };

      for (let i = 0; i < contracts.length; i++) {
        const validationErrors = validate(contracts[i], validationRules);
        if (validationErrors.length > 0) {
          return this.sendError(
            ws,
            `Invalid contract data at index ${i}: Missing required fields`,
            requestId,
            validationErrors
          );
        }
      }

     
      await this.contractModel.bulkCreateContracts(contracts);

  
      this.sendMessage(ws, {
        type: MessageType.CONTRACT_CREATED,
        payload: {
          message: 'Contracts accepted and queued for processing',
          count: contracts.length,
          status: 'queued'
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error queuing contracts:', error);
      this.sendError(ws, 'Failed to create contracts', requestId);
    }
  }

  private async handleGetQueueStatus(ws: WebSocket, requestId?: string) {
    try {
      const stats = this.contractModel.getQueueStats();

      this.sendMessage(ws, {
        type: MessageType.GET_QUEUE_STATUS,
        payload: {
          status: 'success',
          ...stats
        },
        requestId,
      });
    } catch (error) {
      logger.error('Error getting contract queue status:', error);
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

    const contractModel = new ContractModel(clickhouseClient);
    const { address } = req.params;
    
    if (!address) {
      return res.status(400).json({ error: 'Contract address is required' });
    }
    
    const contract = await contractModel.findByAddress(address);
    
    if (!contract) {
      return res.status(404).json({ error: 'Contract not found' });
    }
    
    return res.json(contract);
  } catch (error) {
    logger.error('Error fetching contract:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});


router.post('/create', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const contractModel = new ContractModel(clickhouseClient);
    const contractData = req.body;

   
    const validationRules = {
      contract_address: { required: true, type: 'string' },
      chain: { required: true, type: 'string' },
      symbol: { required: true, type: 'string' },
      token_name: { required: true, type: 'string' },
    };

    const validationErrors = validate(contractData, validationRules);
    if (validationErrors.length > 0) {
      return res.status(400).json({ 
        error: 'Validation failed', 
        details: validationErrors 
      });
    }

  
    await contractModel.createContract(contractData);

    return res.status(202).json({ 
      message: 'Contract accepted and queued for processing',
      status: 'queued'
    });
  } catch (error) {
    logger.error('Error creating contract:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});


router.post('/bulk', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const contractModel = new ContractModel(clickhouseClient);
    const { contracts } = req.body;

    if (!Array.isArray(contracts) || contracts.length === 0) {
      return res.status(400).json({ 
        error: 'Invalid input: Expected non-empty array of contracts'
      });
    }

    
    const validationRules = {
      contract_address: { required: true, type: 'string' },
      chain: { required: true, type: 'string' },
      symbol: { required: true, type: 'string' },
      token_name: { required: true, type: 'string' },
    };

    for (let i = 0; i < contracts.length; i++) {
      const validationErrors = validate(contracts[i], validationRules);
      if (validationErrors.length > 0) {
        return res.status(400).json({
          error: `Invalid contract data at index ${i}: Missing required fields`,
          details: validationErrors
        });
      }
    }


    await contractModel.bulkCreateContracts(contracts);

    return res.status(202).json({
      message: 'Contracts accepted and queued for processing',
      count: contracts.length,
      status: 'queued'
    });
  } catch (error) {
    logger.error('Error bulk creating contracts:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});


router.get('/queue/stats', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const contractModel = new ContractModel(clickhouseClient);
    const stats = contractModel.getQueueStats();
    
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

    const page = parseInt(req.query.page as string) || 1;
    const limit = parseInt(req.query.limit as string) || 20;
    const offset = (page - 1) * limit;
    
    const query = `
      SELECT * FROM contract_info
      ORDER BY token_id DESC
      LIMIT ${limit} OFFSET ${offset}
    `;
    
    const result = await clickhouseClient.query({
      query,
      format: 'JSONEachRow'
    });
    
    const contracts = await result.json();
    
   
    const countQuery = `
      SELECT count() as total FROM contract_info
    `;
    
    const countResult = await clickhouseClient.query({
      query: countQuery,
      format: 'JSONEachRow'
    });
    
    const countData = await countResult.json() as [{ total: number }];
    const total = countData[0]?.total || 0;
    
    return res.json({
      data: contracts,
      pagination: {
        total,
        page,
        limit,
        pages: Math.ceil(total / limit)
      }
    });
  } catch (error) {
    logger.error('Error fetching contracts:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});


router.put('/edit/:contractAddress', async (req, res) => {
  console.log('Editing by contract address');
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const { contractAddress } = req.params;
    const contractData = req.body;

    if (!contractAddress || typeof contractAddress !== 'string') {
      return res.status(400).json({ error: 'Valid contract address is required' });
    }

    const validationRules = {
      contract_address: { required: false, type: 'string' },
      chain: { required: false, type: 'string' },
      symbol: { required: false, type: 'string' },
      token_name: { required: false, type: 'string' },
    };

    const validationErrors = validate(contractData, validationRules);
    if (validationErrors.length > 0) {
      return res.status(400).json({
        error: 'Validation failed',
        details: validationErrors
      });
    }

    const checkQuery = `
      SELECT contract_address FROM contract_info
      WHERE contract_address = '${contractAddress}'
      LIMIT 1
    `;

    const checkResult = await clickhouseClient.query({
      query: checkQuery,
      format: 'JSONEachRow'
    });

    const existingContract = await checkResult.json();

    if (!existingContract || existingContract.length === 0) {
      return res.status(404).json({ error: 'Contract not found' });
    }

    const updateFields = [];

    const now = new Date()
      .toISOString()
      .replace('T', ' ')
      .replace('Z', '')
      .slice(0, 23); // Format: YYYY-MM-DD HH:MM:SS.mmm

    if (contractData.contract_address) updateFields.push(`contract_address = '${contractData.contract_address}'`);
    if (contractData.chain) updateFields.push(`chain = '${contractData.chain}'`);
    if (contractData.symbol) updateFields.push(`symbol = '${contractData.symbol}'`);
    if (contractData.token_name) updateFields.push(`token_name = '${contractData.token_name}'`);
    updateFields.push(`updated_at = '${now}'`);

    if (updateFields.length === 1 && updateFields[0].startsWith('updated_at')) {
      return res.status(400).json({ error: 'No fields to update' });
    }

    const updateQuery = `
      ALTER TABLE contract_info
      UPDATE ${updateFields.join(', ')}
      WHERE contract_address = '${contractAddress}'
    `;

    await clickhouseClient.query({
      query: updateQuery
    });

    return res.status(200).json({
      message: 'Contract updated successfully',
      contract_address: contractAddress
    });
  } catch (error) {
    logger.error('Error updating contract:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});




router.delete('/delete/:contractAddress', async (req, res) => {
  try {
    if (!clickhouseClient) {
      return res.status(503).json({ error: 'Database connection not available' });
    }

    const { contractAddress } = req.params;

    if (!contractAddress || typeof contractAddress !== 'string') {
      return res.status(400).json({ error: 'Valid contract address is required' });
    }

    // Check if the contract exists
    const checkQuery = `
      SELECT contract_address FROM contract_info
      WHERE contract_address = '${contractAddress}'
      LIMIT 1
    `;

    const checkResult = await clickhouseClient.query({
      query: checkQuery,
      format: 'JSONEachRow'
    });

    const existingContract = await checkResult.json();

    if (!existingContract || existingContract.length === 0) {
      return res.status(404).json({ error: 'Contract not found' });
    }

    // Delete the contract
    const deleteQuery = `
      ALTER TABLE contract_info
      DELETE WHERE contract_address = '${contractAddress}'
    `;

    await clickhouseClient.query({
      query: deleteQuery
    });

    return res.status(200).json({
      message: 'Contract deleted successfully',
      contract_address: contractAddress
    });
  } catch (error) {
    logger.error('Error deleting contract:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;