import logger from '../config/logger2';
import env from './env.utils';
import fetch from 'node-fetch';

export class TelegramService {
  private botToken: string;
  private chatId: string;
  private baseUrl: string;

  constructor() {
    this.botToken = env.TELEGRAM_BOT_TOKEN || '';
    this.chatId = env.TELEGRAM_CHAT_ID || '';
    this.baseUrl = `https://api.telegram.org/bot${this.botToken}`;

    if (!this.botToken || !this.chatId) {
      logger.warn('Telegram bot token or chat ID not configured. Notifications will be disabled.');
    }
  }

  async sendMessage(message: string): Promise<void> {
    if (!this.botToken || !this.chatId) {
      logger.debug('Skipping Telegram notification due to missing configuration');
      return;
    }

    try {
      const response = await fetch(`${this.baseUrl}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: this.chatId,
          text: message,
          parse_mode: 'Markdown',
        }),
      });

      const result = (await response.json()) as { ok: boolean; description?: string };
      if (!result.ok) {
        throw new Error(`Telegram API error: ${result.description}`);
      }
      logger.info('Telegram notification sent successfully');
    } catch (err) {
      logger.error('Failed to send Telegram notification:', err);
    }
  }
}

export default new TelegramService();