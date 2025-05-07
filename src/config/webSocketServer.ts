import WebSocket from 'ws';
import http from 'http';
import { v4 as uuidv4 } from 'uuid';
import { logger } from './logger';
import { ContractsWebSocketHandler } from '../routes/contracts';
import { TokenInfoWebSocketHandler } from '../routes/tokens';
import { PairWebSocketHandler } from '../routes/pairs';
import { SocialInfoWebSocketHandler } from '../routes/social';
import { TokenStatisticsWebSocketHandler } from '../routes/token_stats'; // Import the new handler

interface ExtendedWebSocket extends WebSocket {
  id: string;
  isAlive: boolean;
  lastActivity: number;
  clientType?: 'contract' | 'token' | 'pair' | 'social' | 'tokenStats' | 'unknown'; // Add tokenStats type
}

enum MessageType {
  // Contract message types
  GET_CONTRACT = 'GET_CONTRACT',
  CREATE_CONTRACT = 'CREATE_CONTRACT',
  BULK_CREATE_CONTRACTS = 'BULK_CREATE_CONTRACTS',
  
  // Token message types - new naming convention
  GET_TOKEN_INFO = 'GET_TOKEN_INFO',
  CREATE_TOKEN_INFO = 'CREATE_TOKEN_INFO',
  BULK_CREATE_TOKEN_INFOS = 'BULK_CREATE_TOKEN_INFOS',
  UPDATE_TOKEN_INFO = 'UPDATE_TOKEN_INFO',
  TOKEN_INFO_CREATED = 'TOKEN_INFO_CREATED',
  TOKEN_INFO_UPDATED = 'TOKEN_INFO_UPDATED',
  
  // Legacy token types - for backward compatibility
  GET_TOKEN = 'GET_TOKEN',
  CREATE_TOKEN = 'CREATE_TOKEN',
  BULK_CREATE_TOKENS = 'BULK_CREATE_TOKENS',
  UPDATE_TOKEN = 'UPDATE_TOKEN',
  
  // Pair message types
  GET_PAIR = 'GET_PAIR',
  GET_PAIRS_BY_TOKEN = 'GET_PAIRS_BY_TOKEN',
  CREATE_PAIR = 'CREATE_PAIR',
  BULK_CREATE_PAIRS = 'BULK_CREATE_PAIRS',
  UPDATE_PAIR = 'UPDATE_PAIR',
  DELETE_PAIR = 'DELETE_PAIR',
  GET_TRENDING_PAIRS = 'GET_TRENDING_PAIRS',
  PAIR_CREATED = 'PAIR_CREATED',
  PAIR_UPDATED = 'PAIR_UPDATED',
  
  // Social message types
  GET_SOCIAL_INFO = 'GET_SOCIAL_INFO',
  CREATE_SOCIAL_INFO = 'CREATE_SOCIAL_INFO',
  BULK_CREATE_SOCIAL_INFOS = 'BULK_CREATE_SOCIAL_INFOS',
  UPDATE_SOCIAL_INFO = 'UPDATE_SOCIAL_INFO',
  SOCIAL_INFO_CREATED = 'SOCIAL_INFO_CREATED',
  SOCIAL_INFO_UPDATED = 'SOCIAL_INFO_UPDATED',
  
  // Token Statistics message types
  GET_TOKEN_STATISTICS = 'GET_TOKEN_STATISTICS',
  CREATE_TOKEN_STATISTICS = 'CREATE_TOKEN_STATISTICS',
  BULK_CREATE_TOKEN_STATISTICS = 'BULK_CREATE_TOKEN_STATISTICS',
  UPDATE_TOKEN_STATISTICS = 'UPDATE_TOKEN_STATISTICS',
  TOKEN_STATISTICS_CREATED = 'TOKEN_STATISTICS_CREATED',
  TOKEN_STATISTICS_UPDATED = 'TOKEN_STATISTICS_UPDATED',
  
  // Common message types
  GET_QUEUE_STATUS = 'GET_QUEUE_STATUS',
  ERROR = 'ERROR',
}

interface WebSocketMessage {
  type: MessageType;
  payload: any;
  requestId?: string;
}

export class WebSocketServer {
  private wss: WebSocket.Server;
  private contractsHandler: ContractsWebSocketHandler;
  private tokensHandler: TokenInfoWebSocketHandler;
  private pairsHandler: PairWebSocketHandler;
  private socialHandler: SocialInfoWebSocketHandler;
  private tokenStatsHandler: TokenStatisticsWebSocketHandler; // Add new handler
  private pingInterval: NodeJS.Timeout | null = null;
  private readonly CONNECTION_TIMEOUT = 5 * 60 * 1000; 
  private readonly PING_INTERVAL = 30000; 

  constructor(server: http.Server) {
    this.wss = new WebSocket.Server({ server });
    this.contractsHandler = new ContractsWebSocketHandler();
    this.tokensHandler = new TokenInfoWebSocketHandler();
    this.pairsHandler = new PairWebSocketHandler();
    this.socialHandler = new SocialInfoWebSocketHandler();
    this.tokenStatsHandler = new TokenStatisticsWebSocketHandler(); // Initialize token stats handler
    this.initialize();
  }

  private initialize() {
    this.wss.on('connection', (ws: WebSocket) => {
      const extWs = ws as ExtendedWebSocket;
      extWs.id = uuidv4();
      extWs.isAlive = true;
      extWs.lastActivity = Date.now();
      extWs.clientType = 'unknown';

      logger.info(`WebSocket client connected: ${extWs.id}`);

      extWs.on('pong', () => {
        extWs.isAlive = true;
        extWs.lastActivity = Date.now();
      });

      extWs.on('message', async (message: WebSocket.Data) => {
        extWs.lastActivity = Date.now();
        try {
        
          const parsedMessage = this.parseMessage(message.toString());
          
          if (parsedMessage) {
            const messageType = parsedMessage.type;
            
            if (extWs.clientType === 'unknown') {
              extWs.clientType = this.determineClientType(messageType);
              logger.info(`Client ${extWs.id} identified as ${extWs.clientType} client`);
            }
            
            let processedMessage = message.toString();
            if (this.shouldTransformLegacyMessage(messageType)) {
              processedMessage = this.transformLegacyMessage(message.toString());
            }
            
            try {
              if (this.isContractMessage(messageType)) {
                await this.contractsHandler.handleMessage(ws, processedMessage);
              } else if (this.isTokenMessage(messageType)) {
                await this.tokensHandler.handleMessage(ws, processedMessage);
              } else if (this.isPairMessage(messageType)) {
                await this.pairsHandler.handleMessage(ws, processedMessage);
              } else if (this.isSocialMessage(messageType)) {
                await this.socialHandler.handleMessage(ws, processedMessage);
              } else if (this.isTokenStatsMessage(messageType)) {
                await this.tokenStatsHandler.handleMessage(ws, processedMessage);
              } else if (messageType === MessageType.GET_QUEUE_STATUS) {
               
                if (extWs.clientType === 'contract') {
                  await this.contractsHandler.handleMessage(ws, processedMessage);
                } else if (extWs.clientType === 'token') {
                  await this.tokensHandler.handleMessage(ws, processedMessage);
                } else if (extWs.clientType === 'pair') {
                  await this.pairsHandler.handleMessage(ws, processedMessage);
                } else if (extWs.clientType === 'social') {
                  await this.socialHandler.handleMessage(ws, processedMessage);
                } else if (extWs.clientType === 'tokenStats') {
                  await this.tokenStatsHandler.handleMessage(ws, processedMessage);
                } else {
                  this.sendError(ws, 'Unable to determine queue type', parsedMessage.requestId);
                }
              } else {
                this.sendError(ws, `Unknown message type: ${messageType}`, parsedMessage.requestId);
              }
            } catch (handlerError) {
              logger.error(`Error in message handler for type ${messageType}:`, handlerError);
              this.sendError(ws, `Error processing ${messageType} message`, parsedMessage.requestId);
            }
          } else {
            this.sendError(ws, 'Invalid message format', undefined);
          }
        } catch (error) {
          logger.error('Error handling WebSocket message:', error);
          this.sendError(ws, 'Error processing message', undefined);
        }
      });

      extWs.on('close', () => {
        logger.info(`WebSocket client disconnected: ${extWs.id}`);
      });

      extWs.on('error', (error) => {
        logger.error(`WebSocket error for client ${extWs.id}:`, error);
      });
    });

    this.pingInterval = setInterval(() => {
      this.wss.clients.forEach((ws) => {
        const extWs = ws as ExtendedWebSocket;
        
        if (!extWs.isAlive) {
          logger.info(`Terminating inactive WebSocket connection: ${extWs.id}`);
          extWs.terminate();
          return;
        }

        const inactiveTime = Date.now() - extWs.lastActivity;
        if (inactiveTime > this.CONNECTION_TIMEOUT) {
          logger.info(`Terminating WebSocket connection due to inactivity: ${extWs.id}`);
          extWs.terminate();
          return;
        }

        extWs.isAlive = false;
        extWs.ping(() => {});
      });
    }, this.PING_INTERVAL);

    this.wss.on('close', () => {
      if (this.pingInterval) {
        clearInterval(this.pingInterval);
        this.pingInterval = null;
      }
    });

    logger.info('WebSocket server initialized');
  }

  private parseMessage(message: string): WebSocketMessage | null {
    try {
      const parsed = JSON.parse(message);
      
      // Basic validation
      if (!parsed.type || typeof parsed.type !== 'string') {
        logger.warn('Received message without valid type field');
        return null;
      }
      
      return parsed as WebSocketMessage;
    } catch (error) {
      logger.error('Failed to parse WebSocket message:', error);
      return null;
    }
  }

  private determineClientType(messageType: MessageType): 'contract' | 'token' | 'pair' | 'social' | 'tokenStats' | 'unknown' {
    if (this.isContractMessage(messageType)) {
      return 'contract';
    } else if (this.isTokenMessage(messageType)) {
      return 'token';
    } else if (this.isPairMessage(messageType)) {
      return 'pair';
    } else if (this.isSocialMessage(messageType)) {
      return 'social';
    } else if (this.isTokenStatsMessage(messageType)) {
      return 'tokenStats';
    }
    return 'unknown';
  }

  private isContractMessage(type: MessageType): boolean {
    return [
      MessageType.GET_CONTRACT,
      MessageType.CREATE_CONTRACT,
      MessageType.BULK_CREATE_CONTRACTS
    ].includes(type);
  }

  private isTokenMessage(type: MessageType): boolean {
    return [
      // New naming convention
      MessageType.GET_TOKEN_INFO,
      MessageType.CREATE_TOKEN_INFO,
      MessageType.BULK_CREATE_TOKEN_INFOS,
      MessageType.UPDATE_TOKEN_INFO,
      MessageType.TOKEN_INFO_CREATED,
      MessageType.TOKEN_INFO_UPDATED,
      // Legacy types
      MessageType.GET_TOKEN,
      MessageType.CREATE_TOKEN,
      MessageType.BULK_CREATE_TOKENS,
      MessageType.UPDATE_TOKEN
    ].includes(type);
  }

  private isPairMessage(type: MessageType): boolean {
    return [
      MessageType.GET_PAIR,
      MessageType.GET_PAIRS_BY_TOKEN,
      MessageType.CREATE_PAIR,
      MessageType.BULK_CREATE_PAIRS,
      MessageType.UPDATE_PAIR,
      MessageType.DELETE_PAIR,
      MessageType.GET_TRENDING_PAIRS,
      MessageType.PAIR_CREATED,
      MessageType.PAIR_UPDATED
    ].includes(type);
  }

  private isSocialMessage(type: MessageType): boolean {
    return [
      MessageType.GET_SOCIAL_INFO,
      MessageType.CREATE_SOCIAL_INFO,
      MessageType.BULK_CREATE_SOCIAL_INFOS,
      MessageType.UPDATE_SOCIAL_INFO,
      MessageType.SOCIAL_INFO_CREATED,
      MessageType.SOCIAL_INFO_UPDATED
    ].includes(type);
  }

  private isTokenStatsMessage(type: MessageType): boolean {
    return [
      MessageType.GET_TOKEN_STATISTICS,
      MessageType.CREATE_TOKEN_STATISTICS,
      MessageType.BULK_CREATE_TOKEN_STATISTICS,
      MessageType.UPDATE_TOKEN_STATISTICS,
      MessageType.TOKEN_STATISTICS_CREATED,
      MessageType.TOKEN_STATISTICS_UPDATED
    ].includes(type);
  }

  private shouldTransformLegacyMessage(messageType: MessageType): boolean {
    return [
      MessageType.GET_TOKEN,
      MessageType.CREATE_TOKEN,
      MessageType.BULK_CREATE_TOKENS,
      MessageType.UPDATE_TOKEN
    ].includes(messageType);
  }

  private transformLegacyMessage(message: string): string {
    try {
      const parsed = JSON.parse(message);
      const transformMap: {[key: string]: string} = {
        'GET_TOKEN': 'GET_TOKEN_INFO',
        'CREATE_TOKEN': 'CREATE_TOKEN_INFO',
        'BULK_CREATE_TOKENS': 'BULK_CREATE_TOKEN_INFOS',
        'UPDATE_TOKEN': 'UPDATE_TOKEN_INFO'
      };
      
      if (parsed.type && transformMap[parsed.type]) {
        parsed.type = transformMap[parsed.type];
        logger.info(`Transformed legacy message type ${parsed.type} to ${transformMap[parsed.type]}`);
      }
      
      return JSON.stringify(parsed);
    } catch (error) {
      logger.error('Error transforming legacy message:', error);
      return message; 
    }
  }

  private sendError(ws: WebSocket, errorMessage: string, requestId?: string) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: MessageType.ERROR,
        payload: {
          message: errorMessage
        },
        requestId
      }));
    }
  }

  public getActiveConnections(): number {
    return this.wss.clients.size;
  }

  public getConnectionsByType(): { contracts: number, tokens: number, pairs: number, social: number, tokenStats: number, unknown: number } {
    let contracts = 0;
    let tokens = 0;
    let pairs = 0;
    let social = 0;
    let tokenStats = 0;
    let unknown = 0;

    this.wss.clients.forEach((client) => {
      const extClient = client as ExtendedWebSocket;
      if (extClient.clientType === 'contract') {
        contracts++;
      } else if (extClient.clientType === 'token') {
        tokens++;
      } else if (extClient.clientType === 'pair') {
        pairs++;
      } else if (extClient.clientType === 'social') {
        social++;
      } else if (extClient.clientType === 'tokenStats') {
        tokenStats++;
      } else {
        unknown++;
      }
    });

    return { contracts, tokens, pairs, social, tokenStats, unknown };
  }

  public broadcast(message: any, clientType?: 'contract' | 'token' | 'pair' | 'social' | 'tokenStats'): void {
    const messageStr = JSON.stringify(message);
    this.wss.clients.forEach((client) => {
      const extClient = client as ExtendedWebSocket;
      if (client.readyState === WebSocket.OPEN) {
        if (!clientType || extClient.clientType === clientType) {
          client.send(messageStr);
        }
      }
    });
  }

  public close(): void {
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
      this.pingInterval = null;
    }
    
    this.wss.clients.forEach((client) => {
      const extClient = client as ExtendedWebSocket;
      logger.info(`Closing connection to client: ${extClient.id}`);
      client.terminate();
    });
    
    this.wss.close();
    logger.info('WebSocket server closed');
  }
}