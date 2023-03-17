import { Readable } from 'stream';
import { getRealType, assertNonNullable, checkNonNullable, streamToArray, oldStreamToArray } from '../src';

test('getRealType', () => {
  expect(getRealType(1)).toBe('number');
  expect(getRealType({})).toBe('object');
  expect(getRealType(null)).toBe('null');
});

test('assertNonNullable', () => {
  expect(() => assertNonNullable('abc', undefined)).toThrow('abc is not defined.');
});

test('checkNonNullable', () => {
  expect(checkNonNullable('abc', 123)).toEqual(123);
  expect(() => checkNonNullable('abc', undefined)).toThrow('abc is not defined.');
});

test('streamToArray', async () => {
  const stream = Readable.from([0, 1, 2, 3, 4]);
  expect(await streamToArray(stream)).toEqual([0, 1, 2, 3, 4]);
});

test('oldStreamToArray', async () => {
  // TODO(cristipp) Not sure how to import or create NodeJS.ReadableStream
});
