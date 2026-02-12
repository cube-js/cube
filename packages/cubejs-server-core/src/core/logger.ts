import SqlString from 'sqlstring';
import R from 'ramda';

import type { LogLevel, LoggerFn, LoggerFnParams } from '@cubejs-backend/shared';

export type { LogLevel } from '@cubejs-backend/shared';

type Color = '31' | '32' | '33';

const colors: Record<'red' | 'green' | 'yellow', Color> = {
  red: '31', // ERROR
  green: '32', // INFO
  yellow: '33', // WARNING
};

const withColor = (str: string, color: Color = colors.green): string => `\u001b[${color}m${str}\u001b[0m`;

interface FormatOptions {
  requestId?: string;
  duration?: number;
  query?: string | Record<string, any>;
  values?: any[];
  allSqlLines?: boolean;
  showRestParams?: boolean;
  [key: string]: any;
}

const format = ({ requestId, duration, allSqlLines, query, values, showRestParams, ...json }: FormatOptions): string => {
  const restParams = JSON.stringify(json, null, 2);
  const durationStr = duration ? `(${duration}ms)` : '';
  const prefix = `${requestId || ''} ${durationStr}`.trim();

  if (query && values) {
    const queryMaxLines = 50;
    let queryStr = typeof query === 'string' ? query : JSON.stringify(query);
    queryStr = queryStr.replaceAll(/\$(\d+)/g, '?');
    let formatted = SqlString.format(queryStr, values).split('\n');

    if (formatted.length > queryMaxLines && !allSqlLines) {
      formatted = R.take(queryMaxLines / 2, formatted)
        .concat(['.....', '.....', '.....'])
        .concat(R.takeLast(queryMaxLines / 2, formatted));
    }

    return `${prefix}\n--\n  ${formatted.join('\n')}\n--${showRestParams ? `\n${restParams}` : ''}`;
  } else if (query) {
    return `${prefix}\n--\n${JSON.stringify(query, null, 2)}\n--${showRestParams ? `\n${restParams}` : ''}`;
  }

  return `${prefix}${showRestParams ? `\n${restParams}` : ''}`;
};

export const devLogger = (filterByLevel: LogLevel = 'info') => (type: string, { error, warning, ...restParams }: LoggerFnParams): void => {
  const logWarning = () => console.log(
    `${withColor(type, colors.yellow)}: ${format({ ...restParams, allSqlLines: true, showRestParams: true })} \n${withColor(warning || '', colors.yellow)}`
  );

  const logError = () => console.log(
    `${withColor(type, colors.red)}: ${format({ ...restParams, allSqlLines: true, showRestParams: true })} \n${error}`
  );

  const logDetails = (showRestParams?: boolean) => console.log(
    `${withColor(type)}: ${format({ ...restParams, showRestParams })}`
  );

  if (error) {
    logError();
    return;
  }

  switch (filterByLevel.toLowerCase()) {
    case 'trace': {
      if (!error && !warning) {
        logDetails(true);
      }
      break;
    }
    case 'info': {
      if (!error && !warning && [
        'Executing SQL',
        'Streaming SQL',
        'Executing Load Pre Aggregation SQL',
        'Load Request Success',
        'Performing query',
        'Performing query completed',
        'Streaming successfully completed',
      ].includes(type)) {
        logDetails();
      }
      break;
    }
    case 'warn': {
      if (!error && warning) {
        logWarning();
      }
      break;
    }
    case 'error': {
      if (error) {
        logError();
      }
      break;
    }
    default:
      throw new Error(`Unknown log level: ${filterByLevel}`);
  }
};

export const prodLogger = (filterByLevel: LogLevel = 'warn') => (message: string, params: LoggerFnParams): void => {
  const { error, warning } = params;

  if (!params.level) {
    if (error) {
      params.level = 'error';
    } else if (warning) {
      params.level = 'warn';
    } else {
      params.level = 'info';
    }
  }

  const logMessage = () => {
    const { level, timestamp, ...restParams } = params;

    console.log(JSON.stringify({
      timestamp,
      level,
      message,
      ...restParams,
    }));
  };

  switch (filterByLevel.toLowerCase()) {
    case 'trace': {
      if (!error && !warning) {
        logMessage();
      }
      break;
    }
    case 'info':
      if ([
        'REST API Request',
      ].includes(message)) {
        logMessage();
      }
      break;
    case 'warn': {
      if (!error && warning) {
        logMessage();
      }
      break;
    }
    case 'error': {
      if (error) {
        logMessage();
      }
      break;
    }
    default:
      throw new Error(`Unknown log level: ${filterByLevel}`);
  }
};

export const createLogger = (production: boolean, filterByLevel: LogLevel = 'info'): LoggerFn => {
  if (production) {
    return prodLogger(filterByLevel);
  } else {
    return devLogger(filterByLevel);
  }
};
