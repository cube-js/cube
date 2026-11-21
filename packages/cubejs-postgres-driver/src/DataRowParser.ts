import { createRequire } from 'module';
import type { Duplex } from 'stream';

// Loaded from pg's own dependency tree, so messages match the pg-protocol version pg was built
// against. `handlePacket` is private in its typings, hence the untyped import.
const pgRequire = createRequire(require.resolve('pg'));
const { Parser } = pgRequire('pg-protocol/dist/parser');
const { DataRowMessage } = pgRequire('pg-protocol/dist/messages');

const DATA_ROW = 0x44; // 'D'

/**
 * pg-protocol decodes every DataRow field with its own `Buffer#toString` call. With short text
 * values the per-call cost (C++ binding transition, V8 string allocation) dominates: 7 columns x 1M
 * rows spend ~350 ms there. This parser decodes the whole row with a single call and cuts the
 * fields out with `substring`.
 *
 * Fields are located by byte offsets, which match string offsets only while every byte decodes to
 * exactly one UTF-16 unit. A UTF-8 decoder never turns one byte into more than one unit, so an
 * equal length proves no bytes were merged into a multi-byte character; otherwise (non-ASCII data)
 * the row goes through the regular per-field path. The length headers between fields (`-1` for
 * NULL is `FF FF FF FF`) only ever become single replacement characters, which are never read.
 */
export class DataRowParser extends Parser {
  public handlePacket(offset: number, code: number, length: number, bytes: Buffer) {
    if (code !== DATA_ROW) {
      return super.handlePacket(offset, code, length, bytes);
    }

    // `length` counts its own 4 bytes, `offset` points past them
    const end = offset + length - 4;
    const row = bytes.toString('utf8', offset, end);
    if (row.length !== end - offset) {
      return super.handlePacket(offset, code, length, bytes);
    }

    const fieldCount = bytes.readInt16BE(offset);
    const fields = new Array(fieldCount);
    let pos = offset + 2;

    for (let i = 0; i < fieldCount; i++) {
      const len = bytes.readInt32BE(pos);
      pos += 4;

      if (len === -1) {
        fields[i] = null;
      } else {
        fields[i] = row.substring(pos - offset, pos - offset + len);
        pos += len;
      }
    }

    return new DataRowMessage(length, fields);
  }
}

/**
 * Replacement for pg's `Connection#attachListeners`, identical except for the parser.
 */
export function attachListeners(this: any, stream: Duplex) {
  const parser = new DataRowParser();
  stream.on('data', (buffer: Buffer) => parser.parse(buffer, (msg: any) => {
    const eventName = msg.name === 'error' ? 'errorMessage' : msg.name;
    if (this._emitMessage) {
      this.emit('message', msg);
    }
    this.emit(eventName, msg);
  }));
}
