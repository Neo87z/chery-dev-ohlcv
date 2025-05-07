import winston from 'winston';
import DailyRotateFile from 'winston-daily-rotate-file';
import Transport from 'winston-transport';
import TelegramService from '../services/telegramSender';

class TelegramTransport extends Transport {
  constructor(opts?: Transport.TransportStreamOptions) {
    super(opts);
  }

  log(info: any, callback: () => void) {
    setImmediate(() => {
      this.emit('logged', info);
    });

  
    if (info.level === 'error') {
      const message = `🚨 *Error Log*\n` +
                      `Environment: ${process.env.NODE_ENV || 'unknown'}\n` +
                      `Message: ${info.message}\n` +
                      `Timestamp: ${info.timestamp}\n` +
                      `${info.stack ? `Stack: ${info.stack}` : ''}`;

      TelegramService.sendMessage(message).catch((err) => {
        console.error('Failed to send error log to Telegram:', err);
      });
    }

    callback();
  }
}

const logger = winston.createLogger({
  level: process.env.NODE_ENV === 'production' ? 'info' : 'debug',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json(),
  ),
  transports: [
    new winston.transports.Console(),
    new DailyRotateFile({
      filename: 'logs/error-%DATE%.log',
      datePattern: 'YYYY-MM-DD',
      level: 'error',
      maxFiles: '14d',
    }),
    new DailyRotateFile({
      filename: 'logs/app-%DATE%.log',
      datePattern: 'YYYY-MM-DD',
      maxFiles: '14d',
    }),
    new TelegramTransport({ level: 'error' }),
  ],
});

export default logger;