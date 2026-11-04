import { GraphQLScalarType, Kind } from 'graphql';

// Adapted from graphql-scalars 1.14.1 (MIT, Copyright (c) 2020 The Guild; Copyright (c) 2017 Dirk-Jan Rutten).
// test/graphql-date-time-parity.json pins outcomes and messages against that version.

const DATE_TIME_REGEX = /^(\d{4})-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.\d+)?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$/i;

function isLeapYear(year: number): boolean {
  return (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0;
}

function isValidDateTime(value: string): boolean {
  const match = DATE_TIME_REGEX.exec(value);

  if (!match) {
    return false;
  }

  const [year, month, day] = match.slice(1, 4).map(Number);

  switch (month) {
    case 2:
      return day <= (isLeapYear(year) ? 29 : 28);
    case 4:
    case 6:
    case 9:
    case 11:
      return day <= 30;
    default:
      return true;
  }
}

function fromDate(value: Date): Date {
  if (Number.isNaN(value.getTime())) {
    throw new TypeError('DateTime cannot represent an invalid Date instance');
  }

  return value;
}

function fromString(value: string): Date {
  if (!isValidDateTime(value)) {
    throw new TypeError(`DateTime cannot represent an invalid date-time-string ${value}.`);
  }

  return new Date(value);
}

export const GraphQLDateTime = new GraphQLScalarType({
  name: 'DateTime',
  description: 'A date-time string at UTC, such as 2007-12-03T10:15:30Z, ' +
    'compliant with the `date-time` format outlined in section 5.6 of ' +
    'the RFC 3339 profile of the ISO 8601 standard for representation ' +
    'of dates and times using the Gregorian calendar.',
  serialize(value: unknown) {
    if (value instanceof Date) {
      return fromDate(value);
    }
    if (typeof value === 'string') {
      return fromString(value);
    }
    if (typeof value === 'number') {
      return new Date(value);
    }

    throw new TypeError(`DateTime cannot be serialized from a non string, non numeric or non Date type ${JSON.stringify(value)}`);
  },
  parseValue(value: unknown) {
    if (value instanceof Date) {
      return fromDate(value);
    }
    if (typeof value === 'string') {
      return fromString(value);
    }

    throw new TypeError(`DateTime cannot represent non string or Date type ${JSON.stringify(value)}`);
  },
  parseLiteral(ast) {
    if (ast.kind !== Kind.STRING) {
      throw new TypeError(`DateTime cannot represent non string or Date type ${'value' in ast && ast.value}`);
    }

    return fromString(ast.value);
  },
  extensions: {
    codegenScalarType: 'Date | string',
  },
});
