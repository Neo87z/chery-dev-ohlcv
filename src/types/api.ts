export interface PaginationParams {
  page?: number;
  limit?: number;
  order?: 'asc' | 'desc';
  orderBy?: string;
}

export interface TokenQueryParams extends PaginationParams {
  chain?: string;
  symbol?: string;
  trending?: boolean;
}

export interface PairQueryParams extends PaginationParams {
  token_id?: number;
  base_token?: string;
  quote_token?: string;
}

export interface TradeQueryParams extends PaginationParams {
  pair_id?: number;
  from_date?: string;
  to_date?: string;
  trade_type?: string;
}

export interface CandleQueryParams extends PaginationParams {
  pair_id?: number;
  from_date?: string;
  to_date?: string;
  timeframe: '1m' | '5m' | '15m' | '1h' | '4h' | '1d';
}