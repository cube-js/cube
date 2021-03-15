import SqlString from 'sqlstring';
import R from 'ramda';

interface logParams {
  error?: Error, warning?: string,
  requestId?: string, duration?: string, query?: string, values?: any[],
  msg?: string
};

export function devLogger(level: string) {
  return (message: string, { error, warning, ...rest } : logParams) => {
    const colors = {
      red: '31', // ERROR
      green: '32', // INFO
      yellow: '33', // WARNING
    };

    const withColor = (str?: string, color = colors.green) => `\u001b[${color}m${str}\u001b[0m`;
    const format = ({ requestId, duration, allSqlLines, query, values, showRestParams, ...json }
                    : {requestId?: string, duration?: string, allSqlLines?: boolean, query?: string, values?: any[], showRestParams?: boolean}) => {
      const restParams = JSON.stringify(json, null, 2);
      const durationStr = duration ? `(${duration}ms)` : '';
      const prefix = `${requestId} ${durationStr}`;
      if (query && values) {
        const queryMaxLines = 50;
        query = query.replace(/\$(\d+)/g, '?');
        let formatted = SqlString.format(query, values).split('\n');
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

    const logWarning = () => console.log(
      `${withColor(message, colors.yellow)}: ${format({ ...rest, allSqlLines: true, showRestParams: true })} \n${withColor(warning, colors.yellow)}`
    );
    const logError = () => console.log(`${withColor(message, colors.red)}: ${format({ ...rest, allSqlLines: true, showRestParams: true })} \n${error?.message} \n${error?.stack}`);
    const logDetails = (showRestParams?: boolean) => console.log(`${withColor(message)}: ${format({ ...rest, showRestParams })}`);

    if (error) {
      logError();
      return;
    }

    // eslint-disable-next-line default-case
    switch ((level || 'info').toLowerCase()) {
      case 'trace': {
        if (!error && !warning) {
          logDetails(true);
          break;
        }
      }
      // eslint-disable-next-line no-fallthrough
      case 'info': {
        if (!error && !warning && [
          'Executing SQL',
          'Executing Load Pre Aggregation SQL',
          'Load Request Success',
          'Performing query',
          'Performing query completed',
          'IORedisFactory'
        ].includes(message)) {
          logDetails();
          break;
        }
      }
      // eslint-disable-next-line no-fallthrough
      case 'warn': {
        if (!error && warning) {
          logWarning();
          break;
        }
      }
      // eslint-disable-next-line no-fallthrough
      case 'error': {
        if (error) {
          logError();
          break;
        }
      }
    }
  };
};

export function prodLogger(level: string) {
  return (message: string, { error, warning, ...rest } : logParams) => {

    const logMessage = () => console.log(JSON.stringify({ message: message, error, warning, ...rest }));
    // eslint-disable-next-line default-case
    switch ((level || 'warn').toLowerCase()) {
      case 'trace': {
        if (!error && !warning) {
          logMessage();
          break;
        }
      }
      // eslint-disable-next-line no-fallthrough
      case 'info':
        if ([
          'REST API Request',
        ].includes(message)) {
          logMessage();
          break;
        }
      // eslint-disable-next-line no-fallthrough
      case 'warn': {
        if (!error && warning) {
          logMessage();
          break;
        }
      }
      // eslint-disable-next-line no-fallthrough
      case 'error': {
        if (error) {
          logMessage();
          break;
        }
      }
    }
  };
};
