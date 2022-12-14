import * as stream from 'stream';

/**
 * Data stream.
 */
export class DataStream extends stream.Transform {
  public _transform(chunk, encoding, done) {
    this.push(chunk);
    done();
  }

  public _destroy(error: Error | null, callback: (error: Error | null) => void) {
    this.emit('end');
    super._destroy(error, callback);
  }
}

/**
 * Stream response.
 */
export type StreamResponse = {
  types: {
    name: string,
    type: string
  }[],
  rowStream: DataStream,
  release: () => Promise<DataStream>,
};
