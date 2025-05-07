import { Router, Request, Response, NextFunction } from 'express';
import { setupClickhouse } from '../config/db';
import { TradeModel } from '../models/trade';
import { cacheMiddleware, invalidateCache } from '../middleware/cache';
import { formatResponse } from '../utils/responseFormatter';
import { logger } from '../config/logger';
import { ApiError } from '../middleware/errorHandler';
import { TradeQueryParams } from '../types/api';
import { body, query, validationResult } from 'express-validator';
import { OHLCVQueryParams } from '../models/trade';

const router = Router();

// Initialize trade queue
(async function initialize() {
  try {
    const client = await setupClickhouse();
    TradeModel.initializeQueue(client);
    logger.info('Trade queue initialized successfully');
  } catch (error) {
    logger.error('Failed to initialize trade services:', error);
  }
})();

router.get('/get-all-trades', cacheMiddleware(30), async (req: Request, res: Response, next: NextFunction) => {
  try {
    const params: TradeQueryParams = {
      limit: req.query.limit ? parseInt(req.query.limit as string, 10) : 100,
      page: req.query.page ? parseInt(req.query.page as string, 10) : 1,
      from_date: req.query.from_date as string,
      to_date: req.query.to_date as string,
      trade_type: req.query.trade_type as string,
      order: req.query.order === 'asc' ? 'asc' : 'desc'
    };
    
    const client = await setupClickhouse();
    const tradeModel = new TradeModel(client);
    const trades = await tradeModel.findAll(params);

    const responseData = formatResponse(trades, {
      total: trades.length,
      page: params.page || 1,
      limit: params.limit || 100,
      hasMore: trades.length === (params.limit || 100)
    });

    if (!res.headersSent) {
      res.json(responseData);
    }
  } catch (error) {
    if (!res.headersSent) {
      logger.error('Error in get-all-trades:', error);
      next(error);
    }
  }
});

const validateTrade = [
  body('pair_id').isInt().withMessage('pair_id must be an integer'),
  body('trade_timestamp').isISO8601().withMessage('trade_timestamp must be a valid ISO8601 date'),
  body('trade_type').isString().withMessage('trade_type must be a string'),
  body('price').isString().withMessage('price must be a string'),
  body('amount').isString().withMessage('amount must be a string'),
  body('total').isString().withMessage('total must be a string'),
  body('tx_hash').isString().withMessage('tx_hash must be a string'),
  body('trader_address').isString().withMessage('trader_address must be a string'),
];

router.post('/insert-trade', 
  validateTrade,
  async (req: Request, res: Response, next: NextFunction) => {
    try {
      const tradeData = req.body;
      console.log('Trade data:', tradeData);
      const client = await setupClickhouse();
      const tradeModel = new TradeModel(client);
      
      // Queue the trade for batch processing
      await tradeModel.queueTrade(tradeData);
      
      // Return success response
      res.status(202).json(formatResponse({ 
        message: 'Trade accepted and queued for processing',
        status: 'queued'
      }));
    } catch (error) {
      logger.error('Error queuing trade:', error);
      next(error);
    }
  }
);

router.post('/bulk-insert-trades', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const trades = req.body.trades;
    
    if (!Array.isArray(trades) || trades.length === 0) {
      throw new ApiError('Invalid input: Expected non-empty array of trades', 400);
    }
    
    const client = await setupClickhouse();
    const tradeModel = new TradeModel(client);
    
    // Queue the trades for batch processing
    console.log('Bulk trade data:', trades);
    await tradeModel.queueTrades(trades);
    
    res.status(202).json(formatResponse({ 
      message: 'Trades accepted and queued for processing', 
      count: trades.length,
      status: 'queued'
    }));
  } catch (error) {
    logger.error('Error queuing trades:', error);
    next(error);
  }
});

router.get('/queue-status', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const client = await setupClickhouse();
    const tradeModel = new TradeModel(client);
    const stats = tradeModel.getQueueStats();
    
    res.json(formatResponse({
      status: 'success',
      ...stats
    }));
  } catch (error) {
    logger.error('Error getting queue status:', error);
    next(error);
  }
});

const validateOHLCVBody = [
  body('contract_address').isString().notEmpty().withMessage('contract_address is required'),
  body('limit').optional().isInt({ min: 1 }).withMessage('limit must be a positive integer'),
  body('page').optional().isInt({ min: 1 }).withMessage('page must be a positive integer'),
  body('from_date').optional().isISO8601().withMessage('from_date must be a valid ISO8601 date'),
  body('to_date').optional().isISO8601().withMessage('to_date must be a valid ISO8601 date'),
  body('order').optional().isIn(['asc', 'desc']).withMessage('order must be either asc or desc'),
  body('timeframe').optional().isIn(['1s', '1m', '15m', '1h', '4h', '1d']).withMessage('timeframe must be one of: 1s, 1m, 15m, 1h, 4h, 1d')
];
router.post('/ohlcv', 
  validateOHLCVBody,
  cacheMiddleware(30),
  async (req: Request, res: Response, next: NextFunction) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        // Return detailed validation errors
        logger.error('Validation errors:', JSON.stringify(errors.array()));
        return res.status(400).json({
          success: false,
          message: 'Validation failed',
          errors: errors.array()
        });
      }

      // Extract parameters from request body
      const params: OHLCVQueryParams = {
        contract_address: req.body.contract_address,
        limit: req.body.limit,
        page: req.body.page || 1,
        from_date: req.body.from_date,
        to_date: req.body.to_date,
        order: req.body.order || 'asc',
        timeframe: req.body.timeframe
      };

      // Log the validated params for debugging
      logger.debug('OHLCV params after validation:', params);

      const client = await setupClickhouse();
      const tradeModel = new TradeModel(client);
      const candles = await tradeModel.getOHLCV(params);

      const responseData = {
        success: true,
        data: {
          results: candles,
          meta: {
            contract_address: params.contract_address,
            total: candles.length,
            page: params.page || 1,
            limit: params.limit,
            hasMore: params.limit ? candles.length === params.limit : false
          }
        }
      };

      res.json(responseData);
    } catch (error) {
      logger.error('Error in post-ohlcv:', error);
      
      // Improved error handling
      if (error instanceof ApiError) {
        return res.status(error.statusCode || 500).json({
          success: false,
          message: error.message,
          errors: error.errors
        });
      }
      
      res.status(500).json({
        success: false,
        message: 'Internal server error',
        errors: [{ msg: (error as Error).message }]
      });
    }
  }
);

export default router;