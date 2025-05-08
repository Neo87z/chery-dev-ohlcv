import { Router, Request, Response, NextFunction } from 'express';

//import pairRoutes from './pairs';
import tradeRoutes from './trades';

import { rateLimitMiddleware } from '../middleware/rateLimit';
import { errorHandler, ApiError } from '../middleware/errorHandler';
import { logger } from '../config/logger';

const router = Router();


//router.use(rateLimitMiddleware);



//router.use('/pairs', pairRoutes);
router.use('/trades', tradeRoutes);




router.use((req: Request, res: Response, next: NextFunction) => {
  const message = `Route not found: ${req.originalUrl}`;
  logger.warn(message);
  next(new ApiError(message, 404));
});

router.use((err: any, req: Request, res: Response, next: NextFunction) => {
  const statusCode = err instanceof ApiError ? err.statusCode : 500;
  const message = err.message || 'Internal server error';

  logger.error(' API Error:', {
    method: req.method,
    path: req.originalUrl,
    ip: req.ip,
    statusCode,
    message,
    stack: process.env.NODE_ENV === 'development' ? err.stack : undefined,
  });

  res.status(statusCode).json({
    success: false,
    message,
    ...(process.env.NODE_ENV === 'development' && { stack: err.stack }),
  });
});

export default router;
