import { Request, Response, NextFunction } from 'express';
import { logger } from '../config/logger';

export class ApiError extends Error {
  statusCode: number;
  isOperational: boolean;
  details?: any;
  errors: any;

  constructor(message: string, statusCode: number, details?: any) {
    super(message);
    this.statusCode = statusCode;
    this.name = 'ApiError';
    this.isOperational = true; 
    this.details = details;
    Error.captureStackTrace(this, this.constructor);
  }
}


export const errorHandler = (err: Error, req: Request, res: Response, next: NextFunction) => {

  if (err instanceof ApiError) {
    logger.warn(`API Error: ${err.message}`, {
      path: req.path,
      method: req.method,
      statusCode: err.statusCode,
      details: err.details || 'N/A',
    });

    return res.status(err.statusCode).json({
      error: err.name,
      message: err.message,
      ...(err.details && { details: err.details }),
    });
  }

  if (process.env.NODE_ENV !== 'production') {
    logger.error(`Unhandled error: ${err.message}`, {
      path: req.path,
      method: req.method,
      stack: err.stack, 
    });
  } else {
    logger.error(`Unhandled error: ${err.message}`, {
      path: req.path,
      method: req.method,
    });
  }


  res.status(500).json({
    error: 'InternalServerError',
    message: process.env.NODE_ENV === 'production'
      ? 'An unexpected error occurred'
      : err.message, 
  });
};


export const asyncWrapper = (fn: Function) => (req: Request, res: Response, next: NextFunction) => {
  Promise.resolve(fn(req, res, next)).catch(next); 
};
