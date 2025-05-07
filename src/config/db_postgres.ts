import { Pool } from 'pg';
import { logger } from './logger';
import { TokenInfPostGresModel } from '../models/token_postgres';
import { ContractModel } from '../models/contract_postrgress';
import { SocialInfoModel } from '../models/social_postgress';

const dbConfig = {
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD || 'Betcoin123456*',
  host: process.env.POSTGRES_HOST || 'localhost',
  port: parseInt(process.env.POSTGRES_PORT || '5455'),
  database: process.env.POSTGRES_DATABASE || 'shehanhoradagoda',
  max: parseInt(process.env.POSTGRES_MAX_CLIENTS || '20'),
  idleTimeoutMillis: parseInt(process.env.POSTGRES_IDLE_TIMEOUT || '30000'),
  connectionTimeoutMillis: parseInt(process.env.POSTGRES_CONNECTION_TIMEOUT || '5000'),
};

logger.info('PostgreSQL connection config:', {
  user: dbConfig.user,
  host: dbConfig.host,
  port: dbConfig.port,
  database: dbConfig.database,
});

const pool = new Pool(dbConfig);

pool.on('connect', () => {
  logger.info('Connected to PostgreSQL database');
});

pool.on('error', (err) => {
  logger.error('Unexpected error on idle PostgreSQL client', err);
});

const initDatabase = async (): Promise<boolean> => {
  try {
    logger.info('Attempting to connect to PostgreSQL...');
    const client = await pool.connect();

    // Check and initialize tokens table
    const tokensTableResult = await client.query(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name = 'tokens'
      );
    `);

    if (!tokensTableResult.rows[0].exists) {
      logger.info('Tokens table does not exist, initializing schema...');
      await TokenInfPostGresModel.initializeQueue(pool);
    } else {
      logger.info('Tokens schema validation successful');
    }

    // Refresh tokens materialized view if it exists
    const tokensMatViewResult = await client.query(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name = 'tokens_search_view'
      );
    `);

    if (tokensMatViewResult.rows[0].exists) {
      logger.info('Refreshing tokens materialized view...');
      await client.query('REFRESH MATERIALIZED VIEW CONCURRENTLY tokens_search_view');
    }

    // Check and initialize contracts table
    const contractsTableResult = await client.query(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name = 'contracts'
      );
    `);

    if (!contractsTableResult.rows[0].exists) {
      logger.info('Contracts table does not exist, initializing schema...');
      await ContractModel.initializeQueue(pool);
    } else {
      logger.info('Contracts schema validation successful');
    }

    // Refresh contracts materialized view if it exists
    const contractsMatViewResult = await client.query(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name = 'contracts_search_view'
      );
    `);

    if (contractsMatViewResult.rows[0].exists) {
      logger.info('Refreshing contracts materialized view...');
      await client.query('REFRESH MATERIALIZED VIEW CONCURRENTLY contracts_search_view');
    }

    // Check and initialize social_info table
    const socialInfoTableResult = await client.query(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name = 'social_info'
      );
    `);

    if (!socialInfoTableResult.rows[0].exists) {
      logger.info('Social_info table does not exist, initializing schema...');
      await SocialInfoModel.initializeQueue(pool);
    } else {
      logger.info('Social_info schema validation successful');
    }

    // Refresh social_info materialized view if it exists
    const socialInfoMatViewResult = await client.query(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name = 'social_info_search_view'
      );
    `);

    if (socialInfoMatViewResult.rows[0].exists) {
      logger.info('Refreshing social_info materialized view...');
      await client.query('REFRESH MATERIALIZED VIEW CONCURRENTLY social_info_search_view');
    }

    client.release();
    logger.info('PostgreSQL database initialization complete');
    return true;
  } catch (error) {
    logger.error('Failed to initialize PostgreSQL database:', error);
    return false;
  }
};

export { pool, initDatabase };