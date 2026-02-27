export type LogLevel = 'trace' | 'info' | 'warn' | 'error';

export type LoggerFnParams = {
  // It's possible to fill timestamp at the place of logging, otherwise, it will be filled in automatically
  timestamp?: string,
  requestId?: string;
  duration?: number;
  query?: string | Record<string, any>;
  values?: any[];
  allSqlLines?: boolean;
  showRestParams?: boolean;
  error?: Error | string;
  trace?: string,
  warning?: string,
  [key: string]: any,
};

export type LoggerFn = (msg: string, params: LoggerFnParams) => void;
