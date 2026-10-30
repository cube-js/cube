import { createRequire } from 'module';

import { DataRowParser } from '../src/DataRowParser';

const { Parser } = createRequire(require.resolve('pg'))('pg-protocol/dist/parser');

type Field = Buffer | null;

function dataRow(fields: Field[]): Buffer {
  const bodyLength = 2 + fields.reduce((acc, f) => acc + 4 + (f ? f.length : 0), 0);
  const message = Buffer.alloc(1 + 4 + bodyLength);
  let pos = message.writeUInt8(0x44, 0);
  pos = message.writeInt32BE(4 + bodyLength, pos);
  pos = message.writeInt16BE(fields.length, pos);

  for (const field of fields) {
    pos = message.writeInt32BE(field ? field.length : -1, pos);
    if (field) {
      pos += field.copy(message, pos);
    }
  }

  return message;
}

function commandComplete(text: string): Buffer {
  const body = Buffer.from(`${text}\0`);
  const message = Buffer.alloc(1 + 4 + body.length);
  message.writeUInt8(0x43, 0);
  message.writeInt32BE(4 + body.length, 1);
  body.copy(message, 5);

  return message;
}

function parseAll(parser: any, chunks: Buffer[]): any[] {
  const messages: any[] = [];

  for (const chunk of chunks) {
    parser.parse(chunk, (msg: any) => messages.push(msg));
  }

  return messages;
}

function splitAt(wire: Buffer, cuts: number[]): Buffer[] {
  const points = [0, ...cuts.filter((c) => c > 0 && c < wire.length).sort((a, b) => a - b), wire.length];
  return points.slice(1).map((end, i) => wire.subarray(points[i], end));
}

function expectSameAsPgProtocol(wire: Buffer, chunks: Buffer[] = [wire]) {
  const expected = parseAll(new Parser(), [wire]);
  const actual = parseAll(new DataRowParser(), chunks);

  expect(actual).toEqual(expected);
  actual.forEach((msg, i) => expect(msg.constructor).toBe(expected[i].constructor));
}

const utf8 = (s: string) => Buffer.from(s);

describe('DataRowParser', () => {
  test('ASCII rows, NULLs and empty values', () => {
    expectSameAsPgProtocol(Buffer.concat([
      dataRow([utf8('1'), utf8('customer_1'), null, utf8(''), utf8('2024-01-14 03:45:00'), utf8('t')]),
      dataRow([null, null]),
      dataRow([]),
      dataRow([utf8('x'.repeat(200)), utf8('y'.repeat(70000))]),
      commandComplete('SELECT 3'),
    ]));
  });

  test('ASCII rows are decoded without the per-field fallback', () => {
    const fallback = jest.spyOn(Parser.prototype, 'handlePacket');

    try {
      const messages = parseAll(new DataRowParser(), [
        dataRow([utf8('1'), null, utf8('x'.repeat(300)), utf8('')]),
      ]);

      expect(messages[0].fields).toEqual(['1', null, 'x'.repeat(300), '']);
      expect(fallback).not.toHaveBeenCalled();
    } finally {
      fallback.mockRestore();
    }
  });

  test('non-ASCII values', () => {
    expectSameAsPgProtocol(Buffer.concat([
      dataRow([utf8('Привет'), utf8('naïve'), utf8('€'), utf8('😀'), null, utf8('ascii')]),
      // A length header byte that is a UTF-8 lead byte (0xE2 = 226) right before a value that
      // starts with continuation bytes
      dataRow([Buffer.concat([Buffer.from([0x82, 0xAC]), utf8('a'.repeat(224))])]),
      // Invalid UTF-8 inside a value
      dataRow([Buffer.from([0x61, 0xFF, 0x62]), Buffer.from([0xC3]), Buffer.from([0xE2, 0x82])]),
    ]));
  });

  test('arbitrary bytes split at arbitrary chunk boundaries', () => {
    // Park-Miller: deterministic; scaled from the top bits, a power-of-two LCG's low bits cycle
    let seed = 42;
    const random = (n: number) => {
      seed = (seed * 48271) % 2147483647;
      return Math.floor((seed / 2147483647) * n);
    };
    const randomField = (): Field => {
      if (random(8) === 0) {
        return null;
      }

      const length = random(5) === 0 ? 120 + random(200) : random(12);
      const highBytes = random(3) === 0;
      return Buffer.from(Array.from({ length }, () => (highBytes ? random(256) : 32 + random(95))));
    };

    const messages: Buffer[] = [];

    for (let i = 0; i < 3000; i++) {
      messages.push(dataRow(Array.from({ length: random(9) }, randomField)));
    }
    messages.push(commandComplete('SELECT 3000'));

    const wire = Buffer.concat(messages);
    expectSameAsPgProtocol(wire, splitAt(wire, Array.from({ length: 400 }, () => random(wire.length))));
  });
});
