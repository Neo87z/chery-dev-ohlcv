import winston from 'winston';
import path from 'path';

const logDir = path.resolve(__dirname, '../logs');

const isProduction = process.env.NODE_ENV === 'production';

const consoleFormat = winston.format.combine(
  winston.format.colorize(),
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  winston.format.printf(({ timestamp, level, message, ...meta }) => {
    return `[${timestamp}] ${level}: ${message} ${
      Object.keys(meta).length ? JSON.stringify(meta, null, 2) : ''
    }`;
  })
);

const fileFormat = winston.format.combine(
  winston.format.timestamp(),
  winston.format.errors({ stack: true }),
  winston.format.json()
);

export const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  defaultMeta: { service: 'Cherry-OHLCV' },
  format: fileFormat,
  transports: [
   
    new winston.transports.Console({
      level: isProduction ? 'warn' : 'debug',
      format: consoleFormat,
    }),

   
    ...(isProduction
      ? [
          new winston.transports.File({
            filename: path.join(logDir, 'error.log'),
            level: 'error',
            handleExceptions: true,
            maxsize: 5 * 1024 * 1024, // 5MB
            maxFiles: 5,
          }),
          new winston.transports.File({
            filename: path.join(logDir, 'combined.log'),
            maxsize: 10 * 1024 * 1024, // 10MB
            maxFiles: 5,
          }),
        ]
      : []),
  ],

  
  exceptionHandlers: isProduction
    ? [
        new winston.transports.File({
          filename: path.join(logDir, 'exceptions.log'),
        }),
      ]
    : [],


  exitOnError: false,
});


process.on('unhandledRejection', (reason) => {
  logger.error('💥 Unhandled Rejection:', { reason });
});
