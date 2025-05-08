import express, { Request, Response, NextFunction } from 'express';
import helmet from 'helmet';
import compression from 'compression';
import cors from 'cors';
import { config } from 'dotenv';
import http from 'http';
import { connectToCherryTracer } from './websockets/pumpFunTracer';
import { setupClickhouse } from './config/db';

// Load environment variables
config();

const app = express();
const PORT = process.env.PORT || 3000;

let clickhouseClient: any;


// ---------- Middleware ----------
app.use(helmet());
app.use(compression());
app.use(cors());
app.use(express.json({ limit: '1mb' }));

// ---------- Routes ----------
app.get('/', (req: Request, res: Response) => {
  res.send('Hello, World!');
});

app.get('/health', async (req: Request, res: Response) => {
  try {
    res.status(200).json({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      clickhouse: clickhouseClient ? 'connected' : 'disconnected'
    });
  } catch (error) {
    console.error('Health check failed:', error instanceof Error ? error.stack : error);
    res.status(500).json({
      status: 'degraded',
      timestamp: new Date().toISOString(),
      message: 'Health check failed'
    });
  }
});

// ---------- Global Error Handler ----------
app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
  console.error(`Unhandled error: ${err.message}`, {
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

// Start the server
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

// Initialize ClickHouse and connect to Cherry Tracer
const initServices = async () => {
  try {
    clickhouseClient = await setupClickhouse();
    if (clickhouseClient) {
      //TradeModel.initializeQueue(clickhouseClient);
      //tradeModel = new TradeModel(clickhouseClient);
      //connectToCherryTracer(tradeModel);
      console.log('Connected to Cherry Tracer with TradeModel');
    } else {
      console.error('ClickHouse initialization failed');
    }
  } catch (error) {
    console.error('Error initializing services:', error instanceof Error ? error.stack : error);
  }
};

initServices();