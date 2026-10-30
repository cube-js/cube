import { describeModelSchemas } from '@cubejs-backend/schema-compiler';

import { JsonSchema, JsonSchemaObject, modelSchemaFromJoi } from '../src/converter';

// The properties editors offer, with their paths: those of every object schema in the manifest. An
// `if` or a `when` case lists the keys it checks without `type: 'object'`, so those are left out.
function offeredProperties(root: JsonSchemaObject): [string, JsonSchema][] {
  const definitions = root.definitions ?? {};
  const resolve = (s: JsonSchema) => (typeof s === 'object' && s.$ref && !s.description ? definitions[s.$ref.split('/').pop()!] : s);
  const seen = new Set<JsonSchema>();
  const found: [string, JsonSchema][] = [];
  const visit = (s: JsonSchema, path: string) => {
    if (typeof s !== 'object' || seen.has(s)) {
      return;
    }
    seen.add(s);

    for (const [key, p] of Object.entries(s.properties ?? {})) {
      if (s.type === 'object') {
        found.push([`${path}.${key}`, p]);
      }
      visit(p, `${path}.${key}`);
    }

    for (const sub of [s.items, s.if, s.then, s.else, s.not, ...(s.anyOf ?? []), ...(s.allOf ?? [])]) {
      if (sub !== undefined) {
        visit(sub, sub === s.items ? `${path}[]` : path);
      }
    }
    if (s.$ref) {
      visit(resolve(s), path);
    }
  };
  visit(root, '');
  return found.map(([path, p]) => [path, resolve(p)]);
}

// Every change to the model language shows up here as a diff of the manifest, for review in the
// Core PR. Update with `yarn unit -u` after checking the diff.
describe('the generated manifest', () => {
  const { schema, stats } = modelSchemaFromJoi(describeModelSchemas());

  it('matches the snapshot', () => {
    expect(schema).toMatchSnapshot();
  });

  // A key that only takes part in a rule, such as `{}` in "one of these is required" or `false` for a
  // forbidden one, has nothing to describe.
  it('describes every key it offers', () => {
    const undescribed = offeredProperties(schema)
      .filter(([, p]) => typeof p === 'object' && Object.keys(p).length && !p.description)
      .map(([path]) => path);
    expect(undescribed).toEqual([]);
  });

  it('marks the same places where it differs from Core', () => {
    expect(stats).toMatchSnapshot();
  });
});
