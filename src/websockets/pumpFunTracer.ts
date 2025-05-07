import WebSocket from 'ws';
import crypto from 'crypto';
import { logger } from '../config/logger';
import telegramService from '../services/telegramSender'; // Importing TelegramService
import { Trade } from '../types/database';
import { TradeModel } from '../models/trade';
import trades from '../routes/trades';

function generateTradeId(): string {
  const uuid = crypto.randomUUID();
  const timestamp = Date.now().toString(36);
  const random = Math.floor(Math.random() * 1000000).toString(36);
  return `${timestamp}-${uuid}-${random}`;
}

export function connectToCherryTracer(tradeModel: TradeModel) {
  const TRACER_URL = 'wss://tracer.cherrypump.com';
  const tracerSocket = new WebSocket(TRACER_URL);

  let tradesQueue: Trade[] = [];

  const processTradesInterval = setInterval(async () => {
    if (tradesQueue.length > 0) {
      const tradesToProcess = [...tradesQueue];
      tradesQueue = [];
      try {
        await tradeModel.queueTrades(tradesToProcess);
        logger.info(`Successfully queued ${tradesToProcess.length} trades for bulk insertion`);
      } catch (err) {
        logger.error('Error queueing trades:', err);
        await telegramService.sendMessage(`❌ Error queueing trades: ${err instanceof Error ? err.message : 'Unknown error'}`);
        tradesQueue.push(...tradesToProcess);
      }
    }
  }, 1000);

  tracerSocket.on('open', () => {
    logger.info('Connection to Cherry Tracer WebSocket successful');
    tracerSocket.send(JSON.stringify({ event: 'subscribe' }));
  });

  const processTrades = (eventType: string, data: any) => {
    const txHash = data?.data?.txHash || data?.txHash || 'unknown';
    const timestamp = new Date().toISOString();
    let tradesAdded = 0;

    if (data && data.data && data.data.prices && Array.isArray(data.data.prices)) {
      data.data.prices.forEach((price: { mint: any; tokenPrice: any; volume: any; type: any }) => {
        if (price && price.mint && price.tokenPrice && price.volume) {
          const trade: Trade = {
            trade_id: generateTradeId(),
            contract_address: price.mint,
            trade_timestamp: timestamp,
            tokenPrice: price.tokenPrice,
            volume: price.volume,
            type: price.type || eventType,
          };
          tradesQueue.push(trade);
          tradesAdded++;
        }
      });
    } else if (
      (eventType === 'pumpswap_pool_created' || eventType === 'orca_pool_created') &&
      data &&
      data.pool &&
      data.pool.mint
    ) {
      const trade: Trade = {
        trade_id: generateTradeId(),
        contract_address: data.pool.mint,
        trade_timestamp: timestamp,
        tokenPrice: data.pool.price || data.pool.tokenPrice || 0,
        volume: data.pool.liquidity || data.pool.volume || 0,
        type: eventType,
      };
      tradesQueue.push(trade);
      tradesAdded++;
    }

    if (tradesAdded > 0) {
      logger.info(`Added ${tradesAdded} ${eventType} trades to queue from txHash: ${txHash}`);
    }
  };

  tracerSocket.on('message', async (data) => {
    try {
      const message = JSON.parse(data.toString());
      switch (message.event) {
        case 'raydium_trade':
          processTrades('raydium', message.data);
          break;
        case 'jupiter':
          processTrades('jupiter', message.data);
          break;
        case 'pumpswap_pool_created':
          processTrades('pumpswap_pool_created', message.data);
          break;
        case 'orca_pool_created':
          processTrades('orca_pool_created', message.data);
          break;
        case 'meotora':
          processTrades('meotora', message.data);
          break;
        case 'pumpswap_trade':
          processTrades('pumpswap_trade', message.data);
          break;
        case 'pumpfun_trade':
          processTrades('pumpfun_trade', message.data);
          break;
        case 'meotora_trade':
          processTrades('meotora_trade', message.data);
          break;
        case 'orca_trade':
          processTrades('orca_trade', message.data);
          break;
        case 'pumpfun':
          logger.info('New Pumpfun Token Event Received');
          break;
        default:
          logger.debug(`Received unknown event type: ${message.event}`);
          break;
      }
    } catch (err) {
      logger.error('Error processing Cherry Tracer message:', err);
      await telegramService.sendMessage(`❌ Error processing Cherry Tracer message: ${err instanceof Error ? err.message : 'Unknown error'}`);
    }
  });

  tracerSocket.on('error', async (err) => {
    logger.error('Connection to Cherry Tracer WebSocket unsuccessful:', err);
    await telegramService.sendMessage(`❌ Cherry Tracer WebSocket connection error: ${err instanceof Error ? err.message : 'Unknown error'}`);
  });

  tracerSocket.on('close', async (code, reason) => {
    logger.warn(`Cherry Tracer WebSocket connection closed with code ${code}${reason ? ': ' + reason : ''}`);
    clearInterval(processTradesInterval);
    if (tradesQueue.length > 0) {
      const remainingTrades = [...tradesQueue];
      try {
        await tradeModel.queueTrades(remainingTrades);
        logger.info(`Successfully queued remaining ${remainingTrades.length} trades`);
      } catch (err) {
        logger.error('Error processing remaining trades:', err);
        await telegramService.sendMessage(`❌ Error processing remaining trades: ${err instanceof Error ? err.message : 'Unknown error'}`);
      }
    }
    logger.info('Attempting to reconnect to Cherry Tracer in 5 seconds...');
    setTimeout(() => connectToCherryTracer(tradeModel), 5000);
  });

  return {
    socket: tracerSocket,
    forceBulkProcess: async () => {
      if (tradesQueue.length > 0) {
        const tradesToProcess = [...tradesQueue];
        trades
        tradesQueue = [];
        try {
          await tradeModel.queueTrades(tradesToProcess);
          logger.info(`Successfully queued ${tradesToProcess.length} trades on force process`);
        } catch (err) {
          logger.error('Error queueing trades on force process:', err);
          await telegramService.sendMessage(`❌ Error queueing trades on force process: ${err instanceof Error ? err.message : 'Unknown error'}`);
          tradesQueue.push(...tradesToProcess);
        }
      }
    },
    close: () => {
      logger.info('Manually closing Cherry Tracer WebSocket connection');
      clearInterval(processTradesInterval);
      tracerSocket.close();
    },
  };
}