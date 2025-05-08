import express, { Request, Response, NextFunction } from 'express';
import helmet from 'helmet';
import compression from 'compression';
import cors from 'cors';
import { config } from 'dotenv';
import { setupClickhouse } from './config/db';
import { logger } from './config/logger';
import routes from './routes';
import { connectToCherryTracer } from './websockets/pumpFunTracer';
import { TradeModel } from './models/trade';
import http from 'http';

// Load environment variables first
config();

// Debug environment
console.log(`====== DEBUG ENVIRONMENT INFO ======`);
console.log(`NODE_ENV: ${process.env.NODE_ENV}`);
console.log(`PORT: ${process.env.PORT}`);
console.log(`CLICKHOUSE_URL: ${process.env.CLICKHOUSE_URL ? 'Set (not showing for security)' : 'Not set'}`);
console.log(`CLICKHOUSE_USER: ${process.env.CLICKHOUSE_USER ? 'Set (not showing for security)' : 'Not set'}`);
console.log(`Memory: ${JSON.stringify(process.memoryUsage())}`);
console.log(`====== END DEBUG INFO ======`);

const app = express();
const PORT = process.env.PORT || 3000;
const OHLCV_UPDATE_INTERVAL_MINUTES = process.env.OHLCV_UPDATE_INTERVAL_MINUTES || 1;

let clickhouseClient: any;
let tradeModel: TradeModel;

// ---------- Middleware ----------
console.log('Setting up Express middleware');
app.use(helmet());
app.use(compression());
app.use(cors());
app.use(express.json({ limit: '1mb' }));

// ---------- Routes ----------
console.log('Setting up Express routes');

// Health endpoint - critical for Heroku
app.get('/health', async (req: Request, res: Response) => {
  console.log('Health check endpoint called');
  try {
    res.status(200).json({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      clickhouse: clickhouseClient ? 'connected' : 'not connected yet',
      env: process.env.NODE_ENV,
      memoryUsage: process.memoryUsage(),
    });
  } catch (error) {
    console.error('Health check failed:', error);
    logger.error('Health check failed:', { error: error instanceof Error ? error.stack : error });
    res.status(500).json({
      status: 'degraded',
      timestamp: new Date().toISOString(),
      message: 'Health check failed',
    });
  }
});

// Simple root endpoint for quick verification
app.get('/', (req: Request, res: Response) => {
  console.log('Root endpoint called');
  res.status(200).send('OHLCV API is running');
});

// Add API routes
app.use('/api', routes);

// ---------- Global Error Handler ----------
console.log('Setting up global error handler');
app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
  console.error(`Unhandled error: ${err.message}`, err.stack);
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
console.log('Creating HTTP server');
const server = http.createServer(app);

// ---------- Startup Routine ----------
const startServer = async () => {
  try {
    console.log(`Starting server: About to listen on port ${PORT}`);
    // Start server immediately to bind to $PORT - THIS IS CRITICAL FOR HEROKU
    server.listen(PORT, () => {
      console.log(`Server successfully bound to port ${PORT}`);
      logger.info(`Server running on port ${PORT}`);
    });

    // Handle initialization errors for the server
    server.on('error', (error: any) => {
      if (error.code === 'EADDRINUSE') {
        console.error(`Port ${PORT} is already in use`);
        logger.error(`Port ${PORT} is already in use`);
      } else {
        console.error('Server error:', error);
        logger.error('Server error:', error);
      }
      process.exit(1);
    });

    // Perform heavy initialization after server is started
    console.log('Server started, now initializing ClickHouse connection');
    try {
      clickhouseClient = await setupClickhouse();
      if (!clickhouseClient) {
        console.error('ClickHouse initialization returned null or undefined');
        throw new Error('ClickHouse initialization failed');
      }
      console.log('ClickHouse client initialized successfully');
      logger.info('ClickHouse client initialized');
    } catch (dbError) {
      console.error('ClickHouse initialization error:', dbError);
      logger.error('ClickHouse initialization failed:', dbError);
      // Continue running even if ClickHouse fails - don't exit process
    }

    // Initialize ClickHouse models if client was created
    if (clickhouseClient) {
      console.log('Initializing TradeModel queue');
      try {
        TradeModel.initializeQueue(clickhouseClient);
        tradeModel = new TradeModel(clickhouseClient);
        console.log('TradeModel initialized successfully');
        logger.info('ClickHouse trade model initialized');
      } catch (modelError) {
        console.error('TradeModel initialization error:', modelError);
        logger.error('TradeModel initialization failed:', modelError);
      }
    }

    // Connect to Cherry Tracer - wrap in try/catch to prevent crashing
    console.log('Attempting to connect to Cherry Tracer');
    try {
      connectToCherryTracer(tradeModel);
      console.log('Successfully connected to Cherry Tracer');
      logger.info('Connected to Cherry Tracer');
    } catch (tracerError) {
      console.error('Cherry Tracer connection error:', tracerError);
      logger.error('Cherry Tracer connection failed:', tracerError);
      // Continue running even if this fails
    }

    console.log('Startup completed successfully');
    logger.info(`OHLCV service initialized with ${OHLCV_UPDATE_INTERVAL_MINUTES} minute update interval`);
  } catch (error) {
    console.error('Fatal error during startup:', error);
    logger.error('Fatal error during startup:', error);
    // Don't exit - let Heroku handle restarts if needed
  }
};

// ---------- Global Error Listeners ----------
process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err);
  logger.error('Uncaught Exception:', err);
  // Don't exit immediately on Heroku - give time for logs to flush
  setTimeout(() => {
    process.exit(1);
  }, 1000);
});

process.on('unhandledRejection', (reason) => {
  console.error('Unhandled Rejection:', reason);
  logger.error('Unhandled Rejection:', reason);
  // Don't exit immediately on Heroku - give time for logs to flush
  setTimeout(() => {
    process.exit(1);
  }, 1000);
});

console.log('Starting the server initialization process');
startServer();