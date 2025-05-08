import express, { Request, Response, NextFunction } from 'express';
import helmet from 'helmet';
import compression from 'compression';
import cors from 'cors';
import { config } from 'dotenv';
import { setupClickhouse } from './config/db';
import { logger } from './config/logger';
import routes from './routes';
import { connectToCherryTracer } from './websockets/pumpFunTracer';
import { TradeModel } from '../src/models/trade';
import http from 'http';

config();

const app = express();
const PORT = process.env.PORT || 3000;
const OHLCV_UPDATE_INTERVAL_MINUTES = process.env.OHLCV_UPDATE_INTERVAL_MINUTES || 1;

let clickhouseClient: any;
let tradeModel: TradeModel;

// ---------- Middleware ----------
app.use(helmet());
app.use(compression());
app.use(cors());
app.use(express.json({ limit: '1mb' }));

// ---------- Routes ----------
app.get('/health', async (req: Request, res: Response) => {
  try {
    res.status(200).json({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      clickhouse: clickhouseClient ? 'connected' : 'disconnected',
    });
  } catch (error) {
    logger.error('Health check failed:', { error: error instanceof Error ? error.stack : error });
    res.status(500).json({
      status: 'degraded',
      timestamp: new Date().toISOString(),
      message: 'Health check failed',
    });
  }
});

app.use('/api', routes);

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

// ---------- Server Setup ----------
const server = http.createServer(app);

// Set server timeouts
server.keepAliveTimeout = 65000; // 65 seconds
server.headersTimeout = 66000; // 66 seconds

// Add keep-alive header
app.use((req, res, next) => {
  res.setHeader('Connection', 'keep-alive');
  next();
});

// ---------- Startup Routine ----------
const startServer = async () => {
  try {
    // Start server immediately and bind to $PORT (important for Heroku)
    server.listen(PORT, () => {
      logger.info(`Server running on port ${PORT}`);
    });

    // Perform heavy initialization concurrently
    logger.info('Initializing ClickHouse client...');
    clickhouseClient = await setupClickhouse();
    if (!clickhouseClient) throw new Error('ClickHouse initialization failed');
    logger.info('ClickHouse client initialized');

    // Initialize ClickHouse models
    TradeModel.initializeQueue(clickhouseClient);
    tradeModel = new TradeModel(clickhouseClient);
    logger.info('ClickHouse trade model initialized');

    // Connect to Cherry Tracer
    connectToCherryTracer(tradeModel);
    logger.info('Connected to Cherry Tracer');

    logger.info(`OHLCV service initialized with ${OHLCV_UPDATE_INTERVAL_MINUTES} minute update interval`);
  } catch (error) {
    logger.error('Fatal error during startup:', error);
    // Don't exit the process on initialization errors since server is already running
    // This prevents Heroku from cycling the dyno unnecessarily
    // Just log the error and continue with limited functionality
    logger.error('Server running with limited functionality due to initialization error');
  }
};

// ---------- Global Error Listeners ----------
process.on('uncaughtException', (err) => {
  logger.error('Uncaught Exception:', err);
  // In production, we might want to keep the server running despite errors
  if (process.env.NODE_ENV !== 'production') {
    process.exit(1);
  }
});

process.on('unhandledRejection', (reason) => {
  logger.error('Unhandled Rejection:', reason);
  // In production, we might want to keep the server running despite errors
  if (process.env.NODE_ENV !== 'production') {
    process.exit(1);
  }
});

// Listen for termination signals
process.on('SIGTERM', () => {
  logger.info('SIGTERM received. Graceful shutdown started...');
  server.close(() => {
    logger.info('Server closed successfully');
    process.exit(0);
  });
});

startServer();