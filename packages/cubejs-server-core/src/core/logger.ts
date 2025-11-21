import SqlString from 'sqlstring';
import R from 'ramda';

export type LogLevel = 'trace' | 'info' | 'warn' | 'error';

interface BaseLogMessage {
  requestId?: string;
  duration?: number;
  query?: string | Record<string, any>;
  values?: any[];
  allSqlLines?: boolean;
  showRestParams?: boolean;
  error?: Error | string;
  warning?: string;
  [key: string]: any;
}

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

export const devLogger = (level?: LogLevel) => (type: string, { error, warning, ...message }: BaseLogMessage): void => {
  const logWarning = () => console.log(
    `${withColor(type, colors.yellow)}: ${format({ ...message, allSqlLines: true, showRestParams: true })} \n${withColor(warning || '', colors.yellow)}`
  );

  const logError = () => console.log(
    `${withColor(type, colors.red)}: ${format({ ...message, allSqlLines: true, showRestParams: true })} \n${error}`
  );

  const logDetails = (showRestParams?: boolean) => console.log(
    `${withColor(type)}: ${format({ ...message, showRestParams })}`
  );

  if (error) {
    logError();
    return;
  }

  // eslint-disable-next-line default-case
  switch ((level || 'info').toLowerCase()) {
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
  }
};

interface ProdLogParams {
  error?: Error | string;
  warning?: string;
  [key: string]: any;
}

export const prodLogger = (level?: LogLevel) => (msg: string, params: ProdLogParams): void => {
  const { error, warning } = params;

  const logMessage = () => console.log(JSON.stringify({ message: msg, ...params }));

  // eslint-disable-next-line default-case
  switch ((level || 'warn').toLowerCase()) {
    case 'trace': {
      if (!error && !warning) {
        logMessage();
      }
      break;
    }
    case 'info':
      if ([
        'REST API Request',
      ].includes(msg)) {
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
  }
};
