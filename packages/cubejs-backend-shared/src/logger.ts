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
  level?: LogLevel,
  [key: string]: any,
};

export type LoggerFn = (msg: string, params: LoggerFnParams) => void;

function testLogLevel(): LogLevel {
  const level = process.env.CUBEJS_TEST_LOG_LEVEL;
  if (level) {
    return level as LogLevel;
  }

  // GitHub Actions sets RUNNER_DEBUG when a job is re-run with debug logging enabled
  return process.env.RUNNER_DEBUG === '1' ? 'trace' : 'error';
}

/**
 * Logger for unit tests: prints only errors, so the thousands of info entries a scheduler or
 * queue test emits don't bury the failures. Set CUBEJS_TEST_LOG_LEVEL (or re-run the CI job
 * with debug logging) to see more.
 */
export const testLogger: LoggerFn = (msg, params) => {
  switch (testLogLevel()) {
    case 'error':
      if (!params?.error) {
        return;
      }
      break;
    case 'warn':
      if (!params?.error && !params?.warning) {
        return;
      }
      break;
    default:
      break;
  }

  console.log(msg, params);
};
