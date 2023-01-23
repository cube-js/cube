import * as stream from 'stream';
import { getEnv } from '@cubejs-backend/shared';

export class QueryStream extends stream.Transform {
  private counter = 0;

  public queryKey: string;

  public maps: {
    queued: Map<string, QueryStream>;
    processing: Map<string, QueryStream>;
  };

  public aliasNameToMember: { [alias: string]: string };

  /**
   * @constructor
   */
  public constructor({
    key,
    maps,
    aliasNameToMember,
  }: {
    key: string;
    maps: {
      queued: Map<string, QueryStream>;
      processing: Map<string, QueryStream>;
    };
    aliasNameToMember: { [alias: string]: string };
  }) {
    super({
      objectMode: true,
      highWaterMark: getEnv('dbQueryStreamHighWaterMark'),
    });
    this.queryKey = key;
    this.maps = maps;
    this.aliasNameToMember = aliasNameToMember;
    if (!this.aliasNameToMember) {
      this.emit('error', 'The QueryStream `aliasNameToMember` property is missed.');
    }
  }

  /**
   * @override
   */
  public _transform(chunk, encoding, callback) {
    if (this.maps.queued.has(this.queryKey)) {
      this.maps.queued.delete(this.queryKey);
      this.maps.processing.set(this.queryKey, this);
    }
    const row = {};
    Object.keys(chunk).forEach((alias) => {
      row[this.aliasNameToMember[alias]] = chunk[alias];
    });
    if (this.counter < this.writableHighWaterMark) {
      this.counter++;
      callback(null, row);
    } else {
      this.pause();
      setTimeout(() => {
        this.resume();
        this.counter = 0;
        callback(null, row);
      }, 0);
    }
  }

  /**
   * @override
   */
  public _destroy(error, callback) {
    if (this.maps.queued.has(this.queryKey)) {
      this.maps.queued.delete(this.queryKey);
    }
    if (this.maps.processing.has(this.queryKey)) {
      this.maps.processing.delete(this.queryKey);
    }
    callback(error);
  }
}
