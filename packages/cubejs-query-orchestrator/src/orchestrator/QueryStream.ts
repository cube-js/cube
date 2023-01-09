import * as stream from 'stream';
import { getEnv } from '@cubejs-backend/shared';

/**
 * Data stream class. This stream is uses to pipe data from the data
 * source stream to the gateway stream consumer (sql api, websocket
 * client, etc.).
 */
export class QueryStream extends stream.Writable {
  public queryKey: string;

  public maps: {
    queued: Map<string, QueryStream>;
    processing: Map<string, QueryStream>;
  };

  public aliasNameToMember: { [alias: string]: string };

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
  }

  public _write(chunk, encoding, callback) {
    if (this.maps.queued.has(this.queryKey)) {
      this.maps.queued.delete(this.queryKey);
      this.maps.processing.set(this.queryKey, this);
    }
    const row = {};
    Object.keys(chunk).forEach((alias) => {
      row[this.aliasNameToMember[alias]] = chunk[alias];
    });
    callback();
    this.emit('data', row);
  }

  public _destroy(error, callback) {
    this.emit('end');
    if (this.maps.queued.has(this.queryKey)) {
      this.maps.queued.delete(this.queryKey);
    }
    if (this.maps.processing.has(this.queryKey)) {
      this.maps.processing.delete(this.queryKey);
    }
    super._destroy(error, callback);
  }
}
