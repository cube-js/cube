export type LoggerFnParams = {
  [key: string]: any;
};

export type LoggerFn = (msg: string, params: LoggerFnParams) => void;