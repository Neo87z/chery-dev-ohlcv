export interface ApiResponse<T> {
    success: boolean;
    data?: T;
    error?: string;
    timestamp: string;
    pagination?: {
      total: number;
      page: number;
      limit: number;
      hasMore: boolean;
    };
  }
  
  export function formatResponse<T>(data: T, pagination?: ApiResponse<T>['pagination']): ApiResponse<T> {
    return {
      success: true,
      data,
      timestamp: new Date().toISOString(),
      pagination
    };
  }
  
  export function formatError(message: string): ApiResponse<null> {
    return {
      success: false,
      error: message,
      timestamp: new Date().toISOString()
    };
  }