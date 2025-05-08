import winston from 'winston';
import Transport from 'winston-transport';
import TelegramService from '../services/telegramSender';

// Detect if running on Heroku
const isHeroku = !!process.env.DYNO;
const isDevelopment = process.env.NODE_ENV !== 'production';

// Custom Telegram transport for error notifications
class TelegramTransport extends Transport {
  constructor(opts?: Transport.TransportStreamOptions) {
    super(opts);
  }

  log(info: any, callback: () => void) {
    setImmediate(() => {
      this.emit('logged', info);
    });

    // Only send errors to Telegram
    if (info.level === 'error') {
      const message = `🚨 *Error Log*\n` +
                      `Environment: ${process.env.NODE_ENV || 'unknown'}\n` +
                      `Dyno: ${process.env.DYNO || 'local'}\n` +
                      `Message: ${info.message}\n` +
                      `Timestamp: ${new Date().toISOString()}\n` +
                      `${info.stack ? `Stack: ${info.stack}` : ''}`;

      // Rate limit Telegram notifications on Heroku to avoid spam
      const shouldSend = !this.rateLimited();
      
      if (shouldSend) {
        TelegramService.sendMessage(message).catch((err) => {
          console.error('Failed to send error log to Telegram:', err);
        });
      }
    }

    callback();
  }

  // Simple rate limiting to prevent too many messages on Heroku
  private lastSent: number = 0;
  private readonly MIN_INTERVAL_MS = 60000; // 1 minute minimum between messages

  private rateLimited(): boolean {
    const now = Date.now();
    if (now - this.lastSent < this.MIN_INTERVAL_MS) {
      return true;
    }
    this.lastSent = now;
    return false;
  }
}

// Create format for console output
const consoleFormat = winston.format.combine(
  winston.format.colorize({ all: true }),
  winston.format.timestamp(),
  winston.format.printf(({ timestamp, level, message, ...meta }) => {
    const metaString = Object.keys(meta).length 
      ? JSON.stringify(meta, null, 2) 
      : '';
    return `[${timestamp}] ${level}: ${message} ${metaString}`;
  })
);

// Define transports array
const transports: winston.transport[] = [
  // Console transport - always enabled and formatted for readability
  new winston.transports.Console({
    format: isDevelopment ? consoleFormat : winston.format.simple(),
    level: isDevelopment ? 'debug' : 'info',
  }),
  
  // Telegram transport for error notifications
  new TelegramTransport({ 
    level: 'error',
    handleExceptions: true,
  })
];

// Create and configure the logger
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || (isDevelopment ? 'debug' : 'info'),
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  defaultMeta: { 
    service: 'Cherry-OHLCV',
    environment: process.env.NODE_ENV || 'development',
    dyno: process.env.DYNO || 'local'
  },
  transports,
  // This ensures uncaught exceptions don't crash the app
  exitOnError: false
});

// Log startup information
logger.info('Logger initialized', { 
  isHeroku,
  environment: process.env.NODE_ENV,
  logLevel: logger.level
});

// Add global unhandled rejection handler
process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled Promise Rejection', { 
    reason: reason instanceof Error ? reason.stack : reason,
    promise
  });
});

export default logger;