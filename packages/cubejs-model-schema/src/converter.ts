// Converts Joi's description of Core's model schemas (`describeModelSchemas()`) into a JSON
// Schema (draft-07) of the model language as written in YAML: snake_case keys, named collections
// as lists of items with `name`, code fields as strings. A rule Joi keeps only as a JS function
// becomes its looser base type, so the schema is never stricter than Core.

/** The parts of `Joi.Description` the converter reads. */
export interface JoiDescription {
  type?: string;
  flags?: {
    presence?: 'optional' | 'required' | 'forbidden';
    only?: boolean;
    description?: string;
  };
  preferences?: { convert?: boolean };
  allow?: unknown[];
  keys?: Record<string, JoiDescription>;
  patterns?: { rule: JoiDescription }[];
  items?: JoiDescription[];
  matches?: JoiMatch[];
  rules?: { name: string; args?: { regex?: string; limit?: number } }[];
  whens?: JoiWhen[];
  dependencies?: { rel: string; peers: string[] }[];
}

interface JoiRef {
  path: string[];
  ancestor?: number;
  type?: string;
}

interface JoiCase {
  is: JoiDescription;
  then?: JoiDescription;
  otherwise?: JoiDescription;
}

interface JoiMatch extends Partial<JoiCase> {
  schema?: JoiDescription;
  ref?: JoiRef;
  switch?: JoiCase[];
}

interface JoiWhen extends Partial<JoiCase> {
  ref: JoiRef;
  switch?: JoiCase[];
}

export type JsonSchema = boolean | JsonSchemaObject;

export interface JsonSchemaObject {
  $schema?: string;
  type?: string | string[];
  description?: string;
  deprecated?: boolean;
  enum?: unknown[];
  pattern?: string;
  minItems?: number;
  maxItems?: number;
  minLength?: number;
  maxLength?: number;
  properties?: Record<string, JsonSchema>;
  required?: string[];
  additionalProperties?: boolean;
  items?: JsonSchema;
  anyOf?: JsonSchema[];
  allOf?: JsonSchema[];
  not?: JsonSchema;
  if?: JsonSchema;
  then?: JsonSchema;
  else?: JsonSchema;
  // Where Core checks more than the schema says, so every gap can be counted.
  [marker: `x-core-${string}`]: true;
}

export type ModelSchemaDescriptions = Record<'cube' | 'view' | 'viewGroup', JoiDescription>;

/** How many times each `x-core-*` marker was emitted. */
export type ConversionStats = Record<string, number>;

const snake = (key: string) => key.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`);

// `allow` also carries `{ override: true }` when a rule resets the allowed list.
const values = (allow: unknown[] = []) => allow.filter((v) => !(v && typeof v === 'object' && 'override' in v));

const regexSource = (regex: string) => regex.replace(/^\/(.*)\/[a-z]*$/, '$1');

// The YAML compiler camelizes keys, so Core accepts a key written either way.
const spellings = (key: string) => [...new Set([snake(key), key])];

// `Joi.when('type', …)` refers to a sibling key; any other reference can't become an `if` here.
const siblingKey = (ref: JoiRef) => (
  ref.path.length === 1 && (ref.ancestor ?? 1) === 1 && (!ref.type || ref.type === 'value')
    ? ref.path[0]
    : undefined
);

// `Joi.ref('.type')` in `alternatives().conditional()` refers to a key of the value itself.
const ownKey = (ref: JoiRef) => (
  ref.path.length === 1 && ref.ancestor === 0 && (!ref.type || ref.type === 'value') ? ref.path[0] : undefined
);

// `CubeSymbols.camelCaseTypes` camelizes these values, so `count_distinct` is `countDistinct`.
const CAMELIZED_VALUES = ['type', 'relationship'];

function withSnakeValues(s: JsonSchema): JsonSchema {
  if (typeof s === 'boolean') {
    return s;
  }
  const out = { ...s };
  if (s.enum) {
    out.enum = [...new Set(s.enum.flatMap((v) => (typeof v === 'string' ? [v, snake(v)] : [v])))];
  }
  if (s.anyOf) {
    out.anyOf = s.anyOf.map(withSnakeValues);
  }
  return out;
}

const valuesFor = (key: string, s: JsonSchema) => (CAMELIZED_VALUES.includes(key) ? withSnakeValues(s) : s);

// `key`, written either way, matches `schema`; `required` also demands that it is present.
const keyMatches = (key: string, schema: JsonSchema, required: boolean): JsonSchemaObject => {
  const [one, ...more] = spellings(key).map((k): JsonSchemaObject => (
    required ? { properties: { [k]: schema }, required: [k] } : { properties: { [k]: schema } }
  ));
  return more.length ? { anyOf: [one, ...more] } : one;
};

const keyIn = (key: string, allowed: unknown[]) => keyMatches(key, valuesFor(key, { enum: allowed }), true);

const withSpellings = (key: string, schema: JsonSchema) => Object.fromEntries(spellings(key).map((k) => [k, schema]));

// `name` has to reach every branch that closes its object with additionalProperties: false.
function named(s: JsonSchema): JsonSchema {
  if (typeof s === 'boolean') {
    return s;
  }

  if (s.type === 'object' && s.properties) {
    return {
      ...s,
      properties: { name: { type: 'string' }, ...s.properties },
      required: [...new Set(['name', ...(s.required ?? [])])],
    };
  }

  const out = { ...s };
  if (s.anyOf) {
    out.anyOf = s.anyOf.map(named);
  }
  if (s.then) {
    out.then = named(s.then);
  }
  if (s.else) {
    out.else = named(s.else);
  }
  return out;
}

// What Core does to a model between the YAML and Joi, so the schema accepts the YAML side of it.

// Keys the compiler adds to a cube, view or view group before validating it; users never write them.
const INTERNAL_KEYS = ['fileName', 'rawCubes', 'rawFolders', 'allDefinitions'];

const withoutInternalKeys = (d: JoiDescription): JoiDescription => ({
  ...d,
  keys: Object.fromEntries(Object.entries(d.keys ?? {}).filter(([key]) => !INTERNAL_KEYS.includes(key))),
});

// `CubeSymbols` sets a pre-aggregation's missing `type` to `rollup` before validating it, so the
// `rollup` case of the type switch also takes a pre-aggregation with no `type`.
function withDefaultPreAggregationType(cube: JoiDescription): JoiDescription {
  const copy: JoiDescription = structuredClone(cube);
  // `RollUpSchema` is itself a tree of `condition(fn)` branches, each requiring `type`.
  const optionalType = (d?: JoiDescription) => {
    if (d?.keys?.type) {
      d.keys.type.flags = { ...d.keys.type.flags, presence: 'optional' };
    }

    for (const m of d?.matches ?? []) {
      [m.schema, m.then, m.otherwise].forEach(optionalType);
    }
  };
  const visit = (d?: JoiDescription): boolean => {
    for (const m of d?.matches ?? []) {
      const rollup = m.switch?.find((c) => values(c.is.allow).includes('rollup'));
      if (m.ref?.path[0] === 'type' && rollup) {
        rollup.is.flags = { ...rollup.is.flags, presence: 'optional' };
        optionalType(rollup.then);
        return true;
      }
      if (visit(m.then) || visit(m.otherwise) || visit(m.schema)) {
        return true;
      }
    }
    return false;
  };
  if (!visit(copy.keys?.preAggregations?.patterns?.[0].rule)) {
    throw new Error('The pre-aggregation type switch was not found in the cube schema');
  }
  return copy;
}

class ModelSchemaConverter {
  public readonly stats: ConversionStats = {};

  public modelSchema(described: ModelSchemaDescriptions): JsonSchemaObject {
    return {
      $schema: 'http://json-schema.org/draft-07/schema#',
      type: 'object',
      properties: {
        cubes: this.namedList(withDefaultPreAggregationType(withoutInternalKeys(described.cube))),
        views: this.namedList(withoutInternalKeys(described.view)),
        view_groups: this.namedList(withoutInternalKeys(described.viewGroup)),
      },
      additionalProperties: false,
    };
  }

  private mark(s: JsonSchemaObject, marker: string): JsonSchemaObject {
    this.stats[marker] = (this.stats[marker] ?? 0) + 1;
    s[`x-core-${marker}`] = true;
    return s;
  }

  // A Joi object keyed by user-chosen names is written in YAML as a list of items with `name`.
  private namedList(item: JoiDescription): JsonSchemaObject {
    return { type: 'array', items: named(this.convert(item)) };
  }

  // Without `.strict()`, Joi converts strings such as "true" or "5", so Core accepts them.
  private scalar(type: string, d: JoiDescription): JsonSchemaObject {
    return d.preferences?.convert === false ? { type } : this.mark({ type: [type, 'string'] }, 'converts-string');
  }

  private convert(d?: JoiDescription): JsonSchema {
    if (!d) {
      return {};
    }
    if (d.flags?.presence === 'forbidden') {
      return false;
    }

    let s: JsonSchemaObject;
    switch (d.type) {
      case 'any':
        s = d.flags?.only ? { enum: values(d.allow) } : {};
        break;
      case 'string':
        s = d.flags?.only ? { type: 'string', enum: values(d.allow) } : { type: 'string' };
        break;
      case 'boolean':
        s = this.scalar('boolean', d);
        break;
      case 'number':
        s = this.scalar('number', d);
        break;
      case 'function':
        // Code is a string in YAML, or a list of them (`levels`, `add_group_by`); a `values` list
        // also takes numbers and booleans.
        s = this.mark({ type: ['string', 'array'], items: { type: ['string', 'number', 'boolean'] } }, 'function');
        break;
      case 'array':
        s = this.array(d);
        break;
      case 'object':
        if (d.keys) {
          s = this.object(d);
        } else if (d.patterns) {
          s = this.namedList(d.patterns[0].rule);
        } else {
          s = { type: 'object' };
        }
        break;
      case 'alternatives':
        s = this.alternatives(d);
        break;
      case 'link':
        // A recursive or shared schema; it becomes a `$ref` once shared parts go to `definitions`.
        s = this.mark({}, 'link');
        break;
      default:
        s = this.mark({}, `unknown-type-${d.type}`);
    }

    for (const rule of d.rules ?? []) {
      const limit = rule.args?.limit;
      if (rule.name === 'pattern' && rule.args?.regex) {
        s.pattern = regexSource(rule.args.regex);
      } else if (rule.name === 'custom') {
        this.mark(s, 'custom');
      } else if (limit !== undefined && ['length', 'min', 'max'].includes(rule.name)) {
        const [min, max] = d.type === 'array' ? ['minItems', 'maxItems'] as const : ['minLength', 'maxLength'] as const;
        if (rule.name !== 'max') {
          s[min] = limit;
        }
        if (rule.name !== 'min') {
          s[max] = limit;
        }
      }
    }
    if (d.flags?.description) {
      s.description = d.flags.description;
    }
    // A field's `when` is hoisted to its parent object; anywhere else it's only counted.
    if (d.whens) {
      this.mark(s, 'when');
    }
    return s;
  }

  private array(d: JoiDescription): JsonSchemaObject {
    const items = (d.items ?? []).map((item) => this.convert(item));
    if (items.length > 1) {
      return { type: 'array', items: { anyOf: items } };
    }
    return items.length ? { type: 'array', items: items[0] } : { type: 'array' };
  }

  // `Joi.alternatives().try(…)`, `.conditional(Joi.ref('.type'), …)` and `condition(fn, …)`.
  private alternatives(d: JoiDescription): JsonSchemaObject {
    const s = this.chain(d.matches ?? []);
    if (typeof s === 'boolean') {
      return s ? {} : { not: {} };
    }
    return s;
  }

  // Joi tries the matches in order. A condition that fails with no `otherwise` falls through to
  // the next match, and a value no match takes is rejected.
  private chain(matches: JoiMatch[]): JsonSchema {
    const [m, ...rest] = matches;
    if (!m) {
      return false;
    }
    if (m.schema) {
      const option = this.convert(m.schema);
      return rest.length ? { anyOf: [option, this.chain(rest)] } : option;
    }
    const cases = m.switch ?? (m.is ? [{ is: m.is, then: m.then, otherwise: m.otherwise }] : []);
    return this.cases(m.ref, cases, rest);
  }

  private cases(ref: JoiRef | undefined, cases: JoiCase[], rest: JoiMatch[]): JsonSchema {
    const [c, ...more] = cases;
    if (!c) {
      return this.chain(rest);
    }

    const then = c.then ? this.convert(c.then) : this.chain(rest);
    let otherwise: JsonSchema;
    if (more.length) {
      otherwise = this.cases(ref, more, rest);
    } else {
      otherwise = c.otherwise ? this.convert(c.otherwise) : this.chain(rest);
    }

    const condition = this.condition(ref, c.is);
    if (!condition) {
      // The condition is a JS function: both branches are kept, the choice between them is lost.
      return this.mark({ anyOf: [then, otherwise] }, 'function-condition');
    }
    return { if: condition, then, else: otherwise };
  }

  // The `if` for a Joi condition, or undefined when the condition can't be said exactly: an `if`
  // looser than Joi's would apply `then` to values Joi sends elsewhere.
  private condition(ref: JoiRef | undefined, is: JoiDescription): JsonSchema | undefined {
    if (is.rules?.some((r) => r.name === 'custom')) {
      return undefined;
    }
    if (!ref || (ref.ancestor === 0 && !ref.path.length)) {
      return this.convert(is);
    }
    const key = ownKey(ref);
    return key ? keyMatches(key, valuesFor(key, this.convert(is)), is.flags?.presence === 'required') : undefined;
  }

  private object(d: JoiDescription): JsonSchemaObject {
    const properties: Record<string, JsonSchema> = {};
    const required: string[] = [];
    const allOf: JsonSchema[] = [];

    for (const [key, value] of Object.entries(d.keys ?? {})) {
      const name = snake(key);
      const hoisted = value.whens?.every((w) => siblingKey(w.ref)) ? value.whens : undefined;
      const converted = this.convert(hoisted ? { ...value, whens: undefined } : value);
      const property = valuesFor(key, typeof converted === 'object' && !Object.keys(converted).length ? true : converted);
      properties[name] = property;
      if (key !== name) {
        properties[key] = typeof property === 'object' ? { ...property, deprecated: true } : property;
      }

      for (const w of hoisted ?? []) {
        allOf.push(...this.whenCases(key, w));
      }
      if (value.flags?.presence === 'required') {
        if (key === name) {
          required.push(name);
        } else {
          allOf.push({ anyOf: [{ required: [name] }, { required: [key] }] });
        }
      }
    }

    const s: JsonSchemaObject = { type: 'object', properties, additionalProperties: false };
    if (required.length) {
      s.required = required;
    }

    for (const dep of d.dependencies ?? []) {
      if (dep.rel === 'oxor' || dep.rel === 'nand') {
        s.not = { required: dep.peers.map(snake) };
      }
    }
    if (allOf.length) {
      s.allOf = allOf;
    }
    return s;
  }

  // `field: Joi.when('type', …)` becomes an `if` on the sibling `type` at the parent object.
  private whenCases(key: string, w: JoiWhen): JsonSchemaObject[] {
    const sibling = siblingKey(w.ref) as string;
    const cases: JoiCase[] = w.switch ?? (w.is ? [{ is: w.is, then: w.then }] : []);
    const out: JsonSchemaObject[] = cases.map((c) => ({
      if: keyIn(sibling, values(c.is.allow)),
      then: { properties: withSpellings(key, this.convert(c.then)) },
    }));

    if (w.otherwise) {
      const covered = cases.flatMap((c) => values(c.is.allow));
      out.push({ if: { not: keyIn(sibling, covered) }, then: { properties: withSpellings(key, this.convert(w.otherwise)) } });
    }
    return out;
  }
}

export function modelSchemaFromJoi(described: ModelSchemaDescriptions) {
  const converter = new ModelSchemaConverter();
  const schema = converter.modelSchema(described);
  return { schema, stats: converter.stats };
}
