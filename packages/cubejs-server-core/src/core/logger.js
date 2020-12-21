const SqlString = require('sqlstring');
const R = require('ramda');

export const devLogger = (level) => (type, { error, warning, ...message }) => {
  const colors = {
    red: '31', // ERROR
    green: '32', // INFO
    yellow: '33', // WARNING
  };

  const withColor = (str, color = colors.green) => `\u001b[${color}m${str}\u001b[0m`;
  const format = ({ requestId, duration, allSqlLines, query, values, showRestParams, ...json }) => {
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
    `${withColor(type, colors.yellow)}: ${format({ ...message, allSqlLines: true, showRestParams: true })} \n${withColor(warning, colors.yellow)}`
  );
  const logError = () => console.log(`${withColor(type, colors.red)}: ${format({ ...message, allSqlLines: true, showRestParams: true })} \n${error}`);
  const logDetails = (showRestParams) => console.log(`${withColor(type)}: ${format({ ...message, showRestParams })}`);

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
      ].includes(type)) {
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

export const prodLogger = (level) => (msg, params) => {
  const { error, warning } = params;

  const logMessage = () => console.log(JSON.stringify({ message: msg, ...params }));
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
      ].includes(msg)) {
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
