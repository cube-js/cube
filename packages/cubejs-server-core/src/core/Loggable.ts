/**
 * Loggable class declaration.
 * @copyright Cube Dev
 * @license Apache 2.0
 */

import type { CreateOptions, LoggerFn } from './types';
import { prodLogger, devLogger } from './logger';

/**
 * Loggable class.
 */
export class Loggable {
  /**
   * Core logger.
   */
  public logger: LoggerFn;

  /**
   * Class constructor.
   */
  public constructor(opts: CreateOptions = {}) {
    this.logger = opts.logger || (
      process.env.NODE_ENV !== 'production'
        ? devLogger(process.env.CUBEJS_LOG_LEVEL)
        : prodLogger(process.env.CUBEJS_LOG_LEVEL)
    );
  }
}
