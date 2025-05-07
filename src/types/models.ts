export interface ContractInfo {
    contract_address: string;
    chain: string;
    symbol: string;
    token_name: string;
    token_id: number;
  }
  
  export interface TokenInfo {
    token_id: number;
    create_date: string;
    makers: string[];
    volume: number;
    image_link: string;
    holders: number;
  }
  
  export interface SocialInfo {
    token_id: number;
    website: string;
    telegram: string;
    other_social: string[];
    explorer: string;
    github: string;
  }
  
  export interface TokenStatistics {
    token_id: number;
    marketcap: number;
    is_trending: number;
    is_tracked: number;
    total_supply: number;
    current_price: number;
  }
  
  export interface Pair {
    pair_id: number;
    token_id: number;
    pair_address: string;
    base_token: string;
    quote_token: string;
  }
  
  export interface Trade {
    trade_id: number;
    pair_id: number;
    trade_timestamp: string;
    trade_type: string;
    price: number;
    amount: number;
    total: number;
    tx_hash: string;
    trader_address: string;
  }
  
  export interface TrendingToken {
    token_id: number;
    search_count: number;
    social_mentions: number;
    trending_score: number;
  }
  
  export interface Candle {
    candle_id: number;
    pair_id: number;
    timestamp: string;
    open_price: number;
    high_price: number;
    low_price: number;
    close_price: number;
    volume: number;
    trade_count: number;
    is_complete: number;
  }
  
  export interface CandleProcessingState {
    pair_id: number;
    last_processed_trade_id: number;
    last_1m_candle_timestamp: string;
    last_5m_candle_timestamp: string;
    last_15m_candle_timestamp: string;
    last_1h_candle_timestamp: string;
    last_4h_candle_timestamp: string;
    last_1d_candle_timestamp: string;
  }