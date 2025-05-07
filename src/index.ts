import express, { Request, Response, NextFunction } from 'express';
import helmet from 'helmet';
import compression from 'compression';
import cors from 'cors';
import { config } from 'dotenv';
import { setupServer, setupCluster } from './config/server';
import { setupClickhouse } from './config/db';
import { cacheMiddleware } from './middleware/cache';
import { createSchemaIfNotExists } from './utils/schema';
import { HealthCheckService } from './services/healthCheck';
import { initRedis } from './middleware/cache';
import { logger } from './config/logger';
import routes from './routes';
import { connectToCherryTracer } from './websockets/pumpFunTracer';
import { WebSocketServer } from './config/webSocketServer';
import { TradeModel } from '../src/models/trade';
import http from 'http';
import { pool, initDatabase } from './config/db_postgres';

config();

const app = express();
const PORT = Number(process.env.PORT) || 3000;
const OHLCV_UPDATE_INTERVAL_MINUTES = Number(process.env.OHLCV_UPDATE_INTERVAL_MINUTES) || 1;

let clickhouseClient: any;
let healthCheckService: HealthCheckService;
let websocketServer: WebSocketServer;
let tradeModel: TradeModel;

// ---------- Middleware ----------
app.use(helmet());
app.use(compression());
app.use(cors());
app.use(express.json({ limit: '1mb' }));

// ---------- Routes ----------
app.get('/health', async (req: Request, res: Response) => {
  try {
    if (!healthCheckService) throw new Error('HealthCheckService not initialized');
    const health = await healthCheckService.check();

    if (websocketServer) {
      health.websocket = {
        activeConnections: websocketServer.getActiveConnections(),
      };
    }

    // Add PostgreSQL status to health check
   

    res.status(200).json(health);
  } catch (error) {
    logger.error('Health check failed:', { error: error instanceof Error ? error.stack : error });
    res.status(500).json({
      status: 'degraded',
      timestamp: new Date().toISOString(),
      message: 'Health check failed',
    });
  }
});

app.use('/api', cacheMiddleware(60), routes);

// ---------- Global Error Handler ----------
app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
  logger.error(`Unhandled error: ${err.message}`, {
    path: req.path,
    method: req.method,
    error: err.stack,
  });

  const message = process.env.NODE_ENV === 'production' ? 'An error occurred' : err.message;

  res.status(500).json({
    error: 'Internal server error',
    message,
  });
});

// ---------- Startup Routine ----------
const startServer = async () => {
  try {
    // Initialize PostgreSQL first
  //  await initDatabase();
    logger.info('PostgreSQL database initialized');

    // Initialize ClickHouse and other services
    clickhouseClient = await setupClickhouse();
    if (!clickhouseClient) throw new Error('ClickHouse initialization failed');
    logger.info('ClickHouse client initialized');

    const redisClient = await initRedis();
    healthCheckService = new HealthCheckService(clickhouseClient, redisClient);

    logger.info('ClickHouse schema ensured.');

    // Initialize ClickHouse models
    TradeModel.initializeQueue(clickhouseClient);
    tradeModel = new TradeModel(clickhouseClient);
    logger.info('ClickHouse trade model initialized');

    // Initialize OHLCV service
    logger.info(`OHLCV service initialized with ${OHLCV_UPDATE_INTERVAL_MINUTES} minute update interval`);

    // Connect to Cherry Tracer with PostgreSQL models
    connectToCherryTracer(tradeModel);
    logger.info('Connected to Cherry Tracer');

    if (process.env.NODE_ENV === 'production') {
      setupCluster(app, initWebSocket);
    } else {
      const server = setupServer(app);
      websocketServer = new WebSocketServer(server);
      server.listen(PORT, () => {
        logger.info(`Development server running on http://localhost:${PORT}`);
        logger.info(`WebSocket server available at ws://localhost:${PORT}`);
      });

      process.on('SIGTERM', () => {
        logger.info('SIGTERM received, shutting down...');
        websocketServer.close();
        server.close(() => {
          logger.info('Server closed gracefully.');
          process.exit(0);
        });
      });
    }
  } catch (error) {
    logger.error('Fatal error during startup:', error);
    process.exit(1);
  }
};

// Function to initialize WebSocket server
function initWebSocket(server: http.Server) {
  websocketServer = new WebSocketServer(server);
  return websocketServer;
}

// ---------- Global Error Listeners ----------
process.on('uncaughtException', (err) => {
  logger.error('Uncaught Exception:', err);
  process.exit(1);
});

process.on('unhandledRejection', (reason) => {
  logger.error('Unhandled Rejection:', reason);
  process.exit(1);
});

startServer();