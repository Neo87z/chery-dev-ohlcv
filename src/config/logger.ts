import winston from 'winston';

// Determine environment
const isProduction = process.env.NODE_ENV === 'production';
const isHeroku = !!process.env.DYNO;

// Create a custom format for console output
const consoleFormat = winston.format.combine(
  winston.format.colorize(),
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  winston.format.printf(({ timestamp, level, message, ...meta }) => {
    return `[${timestamp}] ${level}: ${message} ${
      Object.keys(meta).length ? JSON.stringify(meta, null, 2) : ''
    }`;
  })
);

// JSON format for structured logging (better for log aggregation services)
const jsonFormat = winston.format.combine(
  winston.format.timestamp(),
  winston.format.errors({ stack: true }),
  winston.format.json()
);

// Configure transports
const transports = [
  // Console transport - always enabled
  new winston.transports.Console({
    level: process.env.LOG_LEVEL || (isProduction ? 'info' : 'debug'),
    // Use JSON format on Heroku for better log drainage
    format: isHeroku ? jsonFormat : consoleFormat,
  })
];

// Create the logger
export const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  defaultMeta: { 
    service: 'Cherry-OHLCV',
    environment: process.env.NODE_ENV || 'development',
    dyno: process.env.DYNO || 'local'
  },
  format: jsonFormat,
  transports: transports,
  exitOnError: false,
});

// Add runtime logging configuration
logger.info('Logger initialized', { 
  environment: process.env.NODE_ENV || 'development',
  logLevel: process.env.LOG_LEVEL || (isProduction ? 'info' : 'debug'),
  isHeroku: isHeroku
});

// Setup global error handlers
process.on('unhandledRejection', (reason) => {
  logger.error('💥 Unhandled Rejection:', { reason });
});

// Export a helper for changing log levels at runtime if needed
export const setLogLevel = (level: string) => {
  logger.level = level;
  transports.forEach(transport => {
    if (transport instanceof winston.transports.Console) {
      transport.level = level;
    }
  });
  logger.info(`Log level changed to ${level}`);
};