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
  rules?: { name: string; args?: { regex?: string } }[];
  whens?: JoiWhen[];
  dependencies?: { rel: string; peers: string[] }[];
}

interface JoiRef {
  path: string[];
  ancestor?: number;
  type?: string;
}

interface JoiMatch {
  schema?: JoiDescription;
  ref?: JoiRef;
  is?: JoiDescription;
  then?: JoiDescription;
  otherwise?: JoiDescription;
}

interface JoiCase {
  is: JoiDescription;
  then?: JoiDescription;
  otherwise?: JoiDescription;
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
  enum?: unknown[];
  pattern?: string;
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

// `Joi.when('type', …)` refers to a sibling key; any other reference can't become an `if` here.
const siblingKey = (ref: JoiRef) => (
  ref.path.length === 1 && (ref.ancestor ?? 1) === 1 && (!ref.type || ref.type === 'value')
    ? snake(ref.path[0])
    : undefined
);

const keyIn = (key: string, allowed: unknown[]): JsonSchemaObject => ({
  properties: { [key]: { enum: allowed } },
  required: [key],
});

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

// Keys the compiler adds to a cube, view or view group before validating it; users never write them.
const INTERNAL_KEYS = ['fileName', 'rawCubes', 'rawFolders', 'allDefinitions'];

const withoutInternalKeys = (d: JoiDescription): JoiDescription => ({
  ...d,
  keys: Object.fromEntries(Object.entries(d.keys ?? {}).filter(([key]) => !INTERNAL_KEYS.includes(key))),
});

class ModelSchemaConverter {
  public readonly stats: ConversionStats = {};

  public modelSchema(described: ModelSchemaDescriptions): JsonSchemaObject {
    return {
      $schema: 'http://json-schema.org/draft-07/schema#',
      type: 'object',
      properties: {
        cubes: this.namedList(withoutInternalKeys(described.cube)),
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
        // SQL and other code are strings in YAML.
        s = this.mark({ type: 'string' }, 'function');
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
      if (rule.name === 'pattern' && rule.args?.regex) {
        s.pattern = regexSource(rule.args.regex);
      } else if (rule.name === 'custom') {
        this.mark(s, 'custom');
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

  // `Joi.alternatives().conditional(Joi.ref('.type'), …)` and `condition(fn, …)`.
  private alternatives(d: JoiDescription): JsonSchemaObject {
    const matches = d.matches ?? [];
    const conditional = matches.find((m) => !m.schema);

    if (conditional) {
      const key = conditional.ref && siblingKey(conditional.ref);
      const isValues = values(conditional.is?.allow);
      const then = this.convert(conditional.then);
      const otherwise = this.convert(conditional.otherwise);
      if (key && isValues.length) {
        return { if: keyIn(key, isValues), then, else: otherwise };
      }
      // The condition is a JS function: both branches are kept, the choice between them is lost.
      return this.mark({ anyOf: [then, otherwise] }, 'function-condition');
    }

    const options = matches.map((m) => this.convert(m.schema));
    return options.length === 1 && typeof options[0] !== 'boolean' ? options[0] : { anyOf: options };
  }

  private object(d: JoiDescription): JsonSchemaObject {
    const properties: Record<string, JsonSchema> = {};
    const required: string[] = [];
    const allOf: JsonSchema[] = [];

    for (const [key, value] of Object.entries(d.keys ?? {})) {
      const name = snake(key);
      const hoisted = value.whens?.every((w) => siblingKey(w.ref)) ? value.whens : undefined;
      const property = this.convert(hoisted ? { ...value, whens: undefined } : value);
      properties[name] = typeof property === 'object' && !Object.keys(property).length ? true : property;

      for (const w of hoisted ?? []) {
        allOf.push(...this.whenCases(name, w));
      }
      if (value.flags?.presence === 'required') {
        required.push(name);
      }
    }

    const s: JsonSchemaObject = { type: 'object', properties, additionalProperties: false };
    if (required.length) {
      s.required = required;
    }

    for (const dep of d.dependencies ?? []) {
      if (dep.rel === 'oxor') {
        s.not = { required: dep.peers.map(snake) };
      }
    }
    if (allOf.length) {
      s.allOf = allOf;
    }
    return s;
  }

  // `field: Joi.when('type', …)` becomes an `if` on the sibling `type` at the parent object.
  private whenCases(name: string, w: JoiWhen): JsonSchemaObject[] {
    const sibling = siblingKey(w.ref) as string;
    const cases: JoiCase[] = w.switch ?? (w.is ? [{ is: w.is, then: w.then }] : []);
    const out: JsonSchemaObject[] = cases.map((c) => ({
      if: keyIn(sibling, values(c.is.allow)),
      then: { properties: { [name]: this.convert(c.then) } },
    }));

    if (w.otherwise) {
      const covered = cases.flatMap((c) => values(c.is.allow));
      out.push({ if: { not: keyIn(sibling, covered) }, then: { properties: { [name]: this.convert(w.otherwise) } } });
    }
    return out;
  }
}

export function modelSchemaFromJoi(described: ModelSchemaDescriptions) {
  const converter = new ModelSchemaConverter();
  const schema = converter.modelSchema(described);
  return { schema, stats: converter.stats };
}
