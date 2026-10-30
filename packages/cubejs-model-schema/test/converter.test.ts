// Joi forms Core doesn't use yet, so the differential test can't see them: for each, the schema must
// accept every value Joi accepts.
import Ajv from 'ajv';
import Joi from 'joi';
import { describeModelSchemas } from '@cubejs-backend/schema-compiler';

import { JoiDescription, modelSchemaFromJoi } from '../src/converter';

const KEYS: Record<string, Joi.Schema> = {
  flaggedPattern: Joi.string().pattern(/^x$/i),
  invertedPattern: Joi.string().pattern(/^x/, { invert: true }),
  stringLength: Joi.string().min(2).max(3),
  numberLimit: Joi.number().min(3),
  kind: Joi.string(),
  multiWord: Joi.string(),
  insensitive: Joi.when('kind', { is: Joi.string().valid('a').insensitive().required(), then: Joi.string(), otherwise: Joi.forbidden() }),
  firstMatch: Joi.when('kind', { switch: [
    { is: Joi.valid('a', 'b').required(), then: Joi.string() },
    { is: Joi.valid('a').required(), then: Joi.forbidden() },
  ] }),
  broken: Joi.string()
    .when('kind', { is: Joi.valid('a').required(), then: Joi.string(), break: true })
    .when('multiWord', { is: Joi.valid('b').required(), then: Joi.forbidden() }),
  missingSibling: Joi.when('multiWord', { is: Joi.valid('b'), then: Joi.string(), otherwise: Joi.number() }),
  count: Joi.number(),
  flag: Joi.boolean(),
  onlyTrue: Joi.boolean().valid(true),
  strictTrue: Joi.boolean().valid(true).strict(),
  needsValues: Joi.when('multiWord', { is: Joi.valid('needs').required(), then: Joi.array().required() }),
  mode: Joi.string(),
  otherNeeds: Joi.when('mode', { is: Joi.valid('skip'), then: Joi.any(), otherwise: Joi.string().required() }),
  strictNumber: Joi.number().valid(1, 2).strict(),
  level: Joi.string(),
  relaxed: Joi.any()
    .when('level', { is: Joi.valid('a').required(), then: Joi.required() })
    .when('mode', { is: Joi.valid('skip').required(), then: Joi.optional() }),
  sameFlag: Joi.boolean().valid(Joi.ref('flag')),
  sameStrictFlag: Joi.boolean().valid(Joi.ref('flag')).strict(),
  yesFlag: Joi.boolean().valid(true, 'yes'),
  nullableTrue: Joi.boolean().valid(true).allow(null),
  sensitiveTrue: Joi.boolean().valid(true).sensitive(),
};

// In Joi's camelCase; the schema gets each in snake_case and as is.
const SAMPLES: Record<string, unknown>[] = [
  { flaggedPattern: 'X' },
  { invertedPattern: 'abc' },
  { stringLength: '😀' },
  { numberLimit: '5' },
  { kind: 'A', insensitive: 'x' },
  { kind: 'a', firstMatch: 'x' },
  { kind: 'a', multiWord: 'b', broken: 'x' },
  { missingSibling: 'x' },
  { multiWord: 'c', missingSibling: 1 },
  { level: 'a', mode: 'skip' },
  { flag: true, sameFlag: true },
  { flag: true, sameStrictFlag: true },
];

const snakeKeys = (o: Record<string, unknown>) => Object.fromEntries(
  Object.entries(o).map(([k, v]) => [k.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`), v]),
);

describe('the schema of a Joi form Core does not use yet', () => {
  const described = describeModelSchemas();
  const cube: JoiDescription = {
    ...described.cube,
    keys: { ...described.cube.keys, ...Object.fromEntries(Object.entries(KEYS).map(([k, v]) => [k, v.describe()])) },
  };
  const ajv = new Ajv({ strict: false });
  ajv.addSchema(modelSchemaFromJoi({ ...described, cube }).schema, 'manifest');
  const check = ajv.getSchema('manifest#/properties/cubes/items')!;
  const joi = Joi.object(KEYS);

  it.each(SAMPLES)('accepts %j, as Joi does', (sample) => {
    expect(joi.validate(sample).error).toBeUndefined();
    expect(check({ name: 'orders', sql_table: 'orders', ...snakeKeys(sample) })).toBe(true);
    expect(check({ name: 'orders', sql_table: 'orders', ...sample })).toBe(true);
  });

  // Joi converts only some strings; the rest it rejects, and so does the schema.
  it.each([
    { count: ' +.5e3 ' }, { count: '5.' }, { count: '' }, { count: 'abc' }, { count: '0x10' }, { count: 'Infinity' },
    { flag: 'TRUE' }, { flag: 'False' }, { flag: 'yes' }, { flag: '1' },
  ])('agrees with Joi on %j', (sample) => {
    expect(check({ name: 'orders', sql_table: 'orders', ...sample })).toBe(joi.validate(sample).error === undefined);
  });

  it.each([
    { onlyTrue: true }, { onlyTrue: 'TRUE' }, { onlyTrue: false }, { onlyTrue: 'false' },
    { strictTrue: true }, { strictTrue: false }, { strictTrue: 'true' },
    { strictNumber: 1 }, { strictNumber: 3 }, { strictNumber: '1' },
    { multiWord: 'needs', needsValues: [] }, { multiWord: 'needs' },
    { mode: 'full', otherNeeds: 'x' }, { mode: 'full' }, { mode: 'skip' },
    { yesFlag: 'yes' }, { yesFlag: 'YES' }, { yesFlag: 'TRUE' },
    { nullableTrue: null }, { nullableTrue: 'null' },
    { sensitiveTrue: 'true' }, { sensitiveTrue: 'TRUE' },
  ])('agrees with Joi on allowed values and required branches: %j', (sample) => {
    const valid = joi.validate(sample).error === undefined;
    expect(check({ name: 'orders', sql_table: 'orders', ...snakeKeys(sample) })).toBe(valid);
    expect(check({ name: 'orders', sql_table: 'orders', ...sample })).toBe(valid);
  });
});
