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
});
