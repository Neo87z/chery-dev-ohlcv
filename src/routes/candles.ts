import { Router, Request, Response, NextFunction } from 'express';
import { clickhouseClient } from '../config/db';
import { CandleModel } from '../models/candle';
import { cacheMiddleware } from '../middleware/cache';
import { formatResponse, formatError } from '../utils/responseFormatter';
import { logger } from '../config/logger';
import { ApiError } from '../middleware/errorHandler';
import { CandleQueryParams } from '../types/api';

const router = Router();

const isValidDate = (date: string): boolean => {
  return !isNaN(Date.parse(date));
};

router.get('/:timeframe/pair/:pairId', cacheMiddleware(60), async (req: Request, res: Response, next: NextFunction) => {
  const pairId = parseInt(req.params.pairId, 10);
  const timeframe = req.params.timeframe;

  try {
    if (isNaN(pairId)) {
      throw new ApiError('Invalid pair ID', 400);
    }

    if (!['1m', '5m', '15m', '1h', '4h', '1d'].includes(timeframe)) {
      throw new ApiError('Invalid timeframe. Supported values: 1m, 5m, 15m, 1h, 4h, 1d', 400);
    }

    const params: CandleQueryParams = {
      timeframe: timeframe as '1m' | '5m' | '15m' | '1h' | '4h' | '1d',
      limit: req.query.limit ? parseInt(req.query.limit as string, 10) : 100,
      page: req.query.page ? parseInt(req.query.page as string, 10) : 1,
      from_date: req.query.from_date as string,
      to_date: req.query.to_date as string,
      order: req.query.order === 'asc' ? 'asc' : 'desc'
    };

    if (params.from_date && !isValidDate(params.from_date)) {
      throw new ApiError('Invalid from_date parameter, should be a valid date string.', 400);
    }

    if (params.to_date && !isValidDate(params.to_date)) {
      throw new ApiError('Invalid to_date parameter, should be a valid date string.', 400);
    }

    if (!clickhouseClient) {
      throw new ApiError('ClickHouse client is not initialized', 500);
    }
    const candleModel = new CandleModel(clickhouseClient);
    const candles = await candleModel.findByPairId(pairId, params);

    const responseData = formatResponse(candles, {
      total: candles.length,
      page: params.page || 1,
      limit: params.limit || 100,
      hasMore: candles.length === (params.limit || 100)
    });

    res.setHeader('Cache-Control', 'public, max-age=60');
    res.json(responseData);

  } catch (error) {
    logger.error('Error fetching candles', {
      error: error instanceof Error ? error.message : 'Unknown error',
      stack: error instanceof Error ? error.stack : undefined,
      pairId,
      timeframe,
      queryParams: req.query
    });

    next(error);
  }
});

export default router;
