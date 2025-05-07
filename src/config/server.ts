import express from 'express';
import http from 'http';
import cluster from 'cluster';
import os from 'os';
import { logger } from './logger';

const TIMEOUT_MS = 65000;

export function setupServer(app: express.Application): http.Server {
  const server = http.createServer(app);

 
  server.keepAliveTimeout = TIMEOUT_MS;
  server.headersTimeout = TIMEOUT_MS + 1000;
  server.requestTimeout = 30000;
  server.maxConnections = 10000;


  app.use((req, res, next) => {
    res.setHeader('Connection', 'keep-alive');
    next();
  });

 
  setInterval(() => {
    server.getConnections((err, count) => {
      if (!err) {
        logger.info(`Current active connections: ${count}`);
      }
    });
  }, 10000);

  server.on('close', () => {
    logger.info('Server has been closed gracefully');
  });

  return server;
}

export function setupCluster(app: express.Application, initWebSocket?: (server: http.Server) => any): void {
  if (process.env.NODE_ENV === 'production') {
    if (cluster.isPrimary) {
      const numCPUs = os.cpus().length;
      logger.info(`Master ${process.pid} setting up ${numCPUs} workers`);

      for (let i = 0; i < numCPUs; i++) {
        cluster.fork();
      }

     
      cluster.on('exit', (worker, code, signal) => {
        logger.error(`Worker ${worker.process.pid} died. Restarting...`);
        cluster.fork();
      });

      
      const gracefulShutdown = () => {
        logger.info('Shutting down master...');
        for (const id in cluster.workers) {
          cluster.workers[id]?.send('shutdown');
        }
        setTimeout(() => process.exit(1), 10000);
      };

      process.on('SIGTERM', gracefulShutdown);
      process.on('SIGINT', gracefulShutdown);
    } else {
      const server = setupServer(app);
      const PORT = Number(process.env.PORT) || 3000;

      
      let wsServer: { close: () => void; };
      if (initWebSocket) {
        wsServer = initWebSocket(server);
      }

      
      server.listen(PORT, 2048, () => {
        logger.info(`Worker ${process.pid} started on port ${PORT}`);
        if (wsServer) {
          logger.info(`WebSocket server initialized on worker ${process.pid}`);
        }
      });

        
      process.on('message', (msg: string) => {
        if (msg === 'shutdown') {
          logger.info(`Worker ${process.pid} is shutting down gracefully...`);
          if (wsServer && typeof wsServer.close === 'function') {
            wsServer.close();
          }
          server.close(() => {
            logger.info(`Worker ${process.pid} closed successfully`);
            process.exit(0);
          });
        }
      });
    }
  }
}

export function handleServerErrors(app: express.Application): void {

  app.use((err: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
    logger.error(`Error during request: ${err.message}`, {
      stack: err.stack,
      request: {
        method: req.method,
        url: req.url,
        headers: req.headers,
      },
    });
    res.status(500).send('Something went wrong!');
  });


  process.on('unhandledRejection', (reason, promise) => {
    logger.error('Unhandled Promise Rejection:', { reason, promise });
    process.exit(1);
  });

  process.on('uncaughtException', (error) => {
    logger.error('Uncaught Exception:', { error });
    process.exit(1);
  });
}