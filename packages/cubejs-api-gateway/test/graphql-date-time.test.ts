import fs from 'fs';
import { parseValue, printType } from 'graphql';

import { GraphQLDateTime } from '../src/graphql-date-time';

// Outcomes were captured from the package named in the fixture's `source`, not derived from this scalar.
const fixture = JSON.parse(
  fs.readFileSync(`${process.cwd()}/test/graphql-date-time-parity.json`, 'utf-8')
);

// The fixture tags inputs JSON cannot hold, such as `{ "$date": ... }`.
function decodeInput(input: any) {
  if (input === null || typeof input !== 'object') {
    return input;
  }
  if ('$date' in input) {
    return new Date(input.$date);
  }
  if ('$invalidDate' in input) {
    return new Date(NaN);
  }
  if ('$number' in input) {
    return Number(input.$number);
  }
  if ('$undefined' in input) {
    return undefined;
  }
  if ('$bigint' in input) {
    return BigInt(input.$bigint);
  }
  if ('$symbol' in input) {
    return Symbol(input.$symbol);
  }
  if ('$function' in input) {
    return () => undefined;
  }

  return input;
}

function outcomeOf(call: (input: any) => unknown, input: any) {
  try {
    const value = call(input) as Date;

    return { returns: { date: value.toJSON(), sameInstance: value === input } };
  } catch (error: any) {
    return { throws: { name: error.name, message: error.message } };
  }
}

const calls = {
  serialize: (encoded: any) => outcomeOf(GraphQLDateTime.serialize, decodeInput(encoded)),
  parseValue: (encoded: any) => outcomeOf(GraphQLDateTime.parseValue, decodeInput(encoded)),
  parseLiteral: (encoded: string) => outcomeOf(
    (ast) => GraphQLDateTime.parseLiteral(ast, undefined),
    parseValue(encoded)
  ),
};

describe('GraphQLDateTime', () => {
  test('prints the same SDL', () => {
    expect(printType(GraphQLDateTime)).toBe(fixture.sdl);
  });

  test('carries the same extensions', () => {
    expect(GraphQLDateTime.extensions).toEqual(fixture.extensions);
  });

  describe.each(Object.keys(calls))('%s', (method) => {
    const cases = fixture.cases.filter((testCase) => testCase.method === method);

    cases.forEach(({ input, outcome }) => {
      test(JSON.stringify(input), () => {
        expect(calls[method](input)).toEqual(outcome);
      });
    });
  });
});
