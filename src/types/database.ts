export interface ContractInfo {
  contract_address: string;
  chain: string;
  symbol: string;
  token_name: string;
  token_id: number;
  total_supply: number;
  sol_balance: number;    
  usd_balance: number;    
  owner: string;
  decimals: number;
  created_at: string;
  updated_at: string;
}

export interface ContractCreationDto {
  contract_address: string;
  chain: string;
  symbol: string;
  token_name: string;
}

export interface TokenInfo {
  contract_address: string;
  create_date: string;
  makers?: string | string[] | null; // Allow string, string array, or null/undefined
  volume: string;
  image_link: string;
  holders: number;
  description?: string;
}
export interface SocialInfo {
  contract_address: string;
  website: string;
  telegram: string;
  twitter: string;
  github: string;
  explorer: string;
  other_social: string[];
  created_at?: string;
  updated_at?: string;
}


export interface TokenStatistics {
  contract_address: string;
  marketcap: string;
  is_trending: number;
  is_tracked: number;
  total_supply: string;
  current_price: string;
}

export interface Pair {
  pair_id: number;
  contract_address: string;
  base_token: string;
  quote_token: string;
}

  export interface Trade {
    trade_id: string;
    contract_address: string;
    trade_timestamp: string;
    tokenPrice: string;
    volume: string;
    type:string
  }

export interface TrendingToken {
  token_id: number;
  search_count: number;
  social_mentions: number;
  trending_score: string;
}

export interface Candle {
  candle_id: number;
  pair_id: number;
  timestamp: string;
  open_price: string;
  high_price: string;
  low_price: string;
  close_price: string;
  volume: string;
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