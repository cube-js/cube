// Converts Joi's description of Core's model schemas into a JSON Schema (draft-07) of the model
// language as written in YAML. A rule Joi keeps only as a JS function becomes its looser base
// type.
import { withDefinitions } from './definitions';

/** The parts of `Joi.Description` the converter reads. */
export interface JoiDescription {
  type?: string;
  flags?: {
    presence?: 'optional' | 'required' | 'forbidden';
    only?: boolean;
    sensitive?: boolean;
    description?: string;
    id?: string;
  };
  preferences?: { convert?: boolean };
  allow?: unknown[];
  keys?: Record<string, JoiDescription>;
  patterns?: { rule: JoiDescription }[];
  items?: JoiDescription[];
  matches?: JoiMatch[];
  rules?: { name: string; args?: { regex?: string; limit?: unknown; options?: { invert?: boolean } } }[];
  whens?: JoiWhen[];
  dependencies?: JoiDependency[];
  link?: { ref: JoiRef };
  shared?: JoiDescription[];
}

// `.or('a', 'b')` and its kin; `with` and `without` also name the `key` they start from.
interface JoiDependency {
  rel: string;
  peers: string[];
  key?: string;
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
  break?: boolean;
}

export type JsonSchema = boolean | JsonSchemaObject;

export interface JsonSchemaObject {
  $schema?: string;
  $ref?: string;
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
  patternProperties?: Record<string, JsonSchema>;
  required?: string[];
  additionalProperties?: boolean;
  items?: JsonSchema;
  anyOf?: JsonSchema[];
  allOf?: JsonSchema[];
  not?: JsonSchema;
  if?: JsonSchema;
  then?: JsonSchema;
  else?: JsonSchema;
  definitions?: Record<string, JsonSchema>;
}

export type ModelSchemaDescriptions = Record<'cube' | 'view' | 'viewGroup', JoiDescription>;

/** How many places of each kind the schema is looser than Core in. */
export type ConversionStats = Record<string, number>;

const snake = (key: string) => key.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`);

// `allow` also carries `{ override: true }` when a rule resets the allowed list.
const values = (allow: unknown[] = []) => allow.filter((v) => !(v && typeof v === 'object' && 'override' in v));

// Joi writes a regex as `/source/flags`.
const regexParts = (regex: string) => /^\/(.*)\/([a-z]*)$/s.exec(regex)?.slice(1) ?? [regex, ''];

// A `patternProperties` entry for one property name. Validators read these and editors don't offer
// them, which keeps camelCase spellings (the YAML compiler camelizes keys) and hidden keys out of
// completion.
const exactly = (key: string) => `^${key}$`;

// Wrapped rather than copied, so the camelCase spelling shares the snake_case one's definition.
const deprecated = (s: JsonSchema): JsonSchema => (typeof s === 'object' ? { allOf: [s], deprecated: true } : s);

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

// The strings Joi converts: what its `number` type's `numberRx` matches, and `true` or `false` in any
// case. `pattern` applies to strings only.
const CONVERTED_STRINGS: Record<string, string> = {
  number: '^\\s*[+-]?(?:(?:\\d+(?:\\.\\d*)?)|(?:\\.\\d+))(?:[eE][+-]?\\d+)?\\s*$',
  boolean: '^([Tt][Rr][Uu][Ee]|[Ff][Aa][Ll][Ss][Ee])$',
};

const anyCase = (value: unknown) => [...String(value)].map((c) => `[${c.toUpperCase()}${c.toLowerCase()}]`).join('');

// `CubeSymbols.camelCaseTypes` camelizes these values, so `count_distinct` is `countDistinct`.
const CAMELIZED_VALUES = ['type', 'relationship'];

// The camelCase spelling of a value goes to a `pattern`, which editors don't offer.
function withSnakeValues(s: JsonSchema): JsonSchema {
  if (typeof s === 'boolean') {
    return s;
  }
  if (s.anyOf) {
    return { ...s, anyOf: s.anyOf.map(withSnakeValues) };
  }
  if (!s.enum) {
    return s;
  }

  const offered: JsonSchemaObject = { ...s, enum: [...new Set(s.enum.map((v) => (typeof v === 'string' ? snake(v) : v)))] };
  const camel = s.enum.filter((v): v is string => typeof v === 'string' && snake(v) !== v);
  return camel.length ? { anyOf: [offered, { type: 'string', pattern: `^(${camel.join('|')})$` }] } : offered;
}

const valuesFor = (key: string, s: JsonSchema) => (CAMELIZED_VALUES.includes(key) ? withSnakeValues(s) : s);

// `key`, written either way, matches `schema`; `required` also demands that it is present, as
// `not: { properties: { key: false } }`. codemirror-json-schema drops every `required` when it retries
// a completion, and an `if` on a missing `type` would then pick the first branch.
function keyMatches(key: string, schema: JsonSchema, required: boolean): JsonSchemaObject {
  const name = snake(key);
  const snakeCase: JsonSchemaObject = schema === true ? {} : { properties: { [name]: schema } };
  const camel: JsonSchemaObject = schema === true ? {} : { patternProperties: { [exactly(key)]: schema } };
  if (required) {
    snakeCase.not = { properties: { [name]: false } };
    camel.not = { patternProperties: { [exactly(key)]: false } };
  }
  return name === key ? snakeCase : { anyOf: [snakeCase, camel] };
}

const present = (key: string) => keyMatches(key, true, true);

// Not `required`: one of `allowed`, or missing in both spellings rather than in either.
function keyIn(key: string, allowed: unknown[], required = true): JsonSchemaObject {
  const schema = valuesFor(key, { enum: allowed });
  return required || snake(key) === key
    ? keyMatches(key, schema, required)
    : { anyOf: [keyMatches(key, schema, true), { not: present(key) }] };
}

const whenCasesOf = (w: JoiWhen): JoiCase[] => w.switch ?? (w.is ? [{ is: w.is, then: w.then }] : []);

// A switch keeps its `otherwise` on the last case.
const otherwiseOf = (w: JoiWhen) => w.otherwise ?? w.switch?.at(-1)?.otherwise;

// A `when` that `if`s can say exactly: each case is a list of string values of a sibling, no value is
// in two cases (Joi takes the first match), and at most one case also takes a missing sibling, as an
// `is` without `.required()` does.
function isExactWhen(w: JoiWhen): boolean {
  const cases = whenCasesOf(w);
  const allowed = cases.flatMap((c) => values(c.is.allow));
  return siblingKey(w.ref) !== undefined && !w.break
    && cases.every((c) => c.is.flags?.only && !c.is.rules?.length
      && Object.keys(c.is.flags).every((flag) => ['only', 'presence'].includes(flag)))
    && allowed.every((v) => typeof v === 'string') && new Set(allowed).size === allowed.length
    && cases.filter((c) => c.is.flags?.presence !== 'required').length <= 1;
}

const pairs = (keys: string[]) => keys.flatMap((a, i) => keys.slice(i + 1).map((b) => [a, b]));

// The combinations of present (true) and absent (false) keys a Joi dependency rejects.
function rejected({ rel, peers, key }: JoiDependency): [string, boolean][][] {
  const both = ([a, b]: string[]): [string, boolean][] => [[a, true], [b, true]];
  switch (rel) {
    case 'and':
      return peers.flatMap((a) => peers.filter((b) => b !== a).map((b): [string, boolean][] => [[a, true], [b, false]]));
    case 'nand':
      return [peers.map((p): [string, boolean] => [p, true])];
    case 'or':
      return [peers.map((p): [string, boolean] => [p, false])];
    case 'oxor':
      return pairs(peers).map(both);
    case 'xor':
      return [peers.map((p): [string, boolean] => [p, false]), ...pairs(peers).map(both)];
    case 'with':
      return peers.map((p) => [[key as string, true], [p, false]]);
    case 'without':
      return peers.map((p) => [[key as string, true], [p, true]]);
    default:
      throw new Error(`Unknown Joi dependency ${rel}`);
  }
}

// Each rejected combination is a `not` without `required`: codemirror-json-schema drops `required` on a
// completion retry, which would make such a `not` reject everything. Core validates a model that `extends`
// another only after merging the parent in and dropping one of `sql` / `sql_table`, so the rules skip it.
const dependency = (dep: JoiDependency, inherits: boolean): JsonSchemaObject[] => rejected(dep).map((combination) => ({
  not: {
    allOf: [...combination, ...(inherits ? [['extends', false] as [string, boolean]] : [])]
      .map(([k, isPresent]) => (isPresent ? present(k) : { not: present(k) })),
  },
}));

function inEitherSpelling(key: string, schema: JsonSchema): JsonSchemaObject {
  const name = snake(key);
  return name === key
    ? { properties: { [name]: schema } }
    : { properties: { [name]: schema }, patternProperties: { [exactly(key)]: deprecated(schema) } };
}

// An `anyOf` with a `false` branch gets no value completion in codemirror-json-schema.
function anyOf(branches: JsonSchema[]): JsonSchema {
  const open = branches
    .flatMap((b) => (typeof b === 'object' && b.anyOf && Object.keys(b).length === 1 ? b.anyOf : [b]))
    .filter((b) => b !== false);
  if (open.length > 1) {
    return { anyOf: open };
  }
  return open.length ? open[0] : false;
}

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

// Methods `CubeSymbols.createCube` puts on a model. YAML can't write a function, so Core rejects them.
const COMPILER_METHODS = ['rawCubes', 'rawFolders', 'allDefinitions'];

const withoutCompilerMethods = (d: JoiDescription): JoiDescription => ({
  ...d,
  keys: Object.fromEntries(Object.entries(d.keys ?? {}).filter(([key]) => !COMPILER_METHODS.includes(key))),
});

// The compiler overwrites these with its own values after reading the file, so Core takes any value.
const OVERWRITTEN_KEYS = ['fileName', 'isView'];

// Keys Core accepts that editors don't offer: old spellings (`drillMemberReferences` for
// `drillMembers`, `refreshRangeStart` for `buildRangeStart`, the pre-aggregation `*References`), keys
// the compiler sets on members and policies, and the undocumented `fieldType` and `valuesAsSegments`.
const NOT_OFFERED_KEYS = [
  'drillMemberReferences', 'refreshRangeStart', 'refreshRangeEnd', 'measureReferences', 'dimensionReferences',
  'segmentReferences', 'rollupReferences', 'timeDimensionReference', 'memberReference', 'keyReference',
  'includesMembers', 'excludesMembers', 'fieldType', 'valuesAsSegments',
];

// `CubeSymbols` sets a pre-aggregation's missing `type` to `rollup` in cubes and views before
// validating them, so the `rollup` case of the type switch also takes one with no `type`. Core leaves
// an empty one without `type` so that it is rejected; the schema accepts it.
function withDefaultPreAggregationType(cubeOrView: JoiDescription): JoiDescription {
  const copy: JoiDescription = structuredClone(cubeOrView);
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
    throw new Error('The pre-aggregation type switch was not found');
  }
  return copy;
}

class ModelSchemaConverter {
  public readonly stats: ConversionStats = {};

  // Each link target, by the converted schema, with the placeholder its `$ref`s carry until the
  // target gets a name in `definitions`.
  public readonly linkTargets = new Map<JsonSchemaObject, string>();

  // Link targets that sit outside the tree (`Joi.object().shared(…)`), by their Joi id.
  public readonly sharedSchemas: Record<string, JsonSchema> = {};

  // The descriptions being converted, innermost last: what `Joi.link('...')` counts up through.
  protected readonly ancestors: JoiDescription[] = [];

  // A placeholder per target content, so the same target in a cube and in a view gets one name.
  protected readonly placeholders = new Map<string, string>();

  protected readonly pendingTargets = new Map<JoiDescription, string>();

  // Whether the value being converted is a list item rather than the value of a key.
  protected inListItem = false;

  public modelSchema(described: ModelSchemaDescriptions): JsonSchemaObject {
    return {
      $schema: 'http://json-schema.org/draft-07/schema#',
      type: 'object',
      properties: {
        cubes: this.namedList(withDefaultPreAggregationType(withoutCompilerMethods(described.cube))),
        views: this.namedList(withDefaultPreAggregationType(withoutCompilerMethods(described.view))),
        view_groups: this.namedList(withoutCompilerMethods(described.viewGroup)),
      },
      additionalProperties: false,
    };
  }

  // Counted rather than written into the schema, so validators take it in their default strict mode.
  protected mark(s: JsonSchemaObject, kind: string): JsonSchemaObject {
    this.stats[kind] = (this.stats[kind] ?? 0) + 1;
    return s;
  }

  // A Joi object keyed by user-chosen names is written in YAML as a list of items with `name`.
  protected namedList(item: JoiDescription): JsonSchemaObject {
    return { type: 'array', items: named(this.convert(item)) };
  }

  // Without `.strict()`, Joi converts strings such as "true" or "5", so Core accepts them.
  protected scalar(type: string, d: JoiDescription): JsonSchemaObject {
    // A `Joi.ref` among the allowed values can't be listed.
    const only = d.flags?.only && values(d.allow).every((v) => v === null || typeof v !== 'object')
      ? values(d.allow)
      : undefined;
    if (d.preferences?.convert === false) {
      return only ? { enum: only } : { type };
    }
    if (only && type === 'boolean') {
      // Joi converts only the strings of `true` and `false`; other allowed values match as written.
      const spelled = only.filter((v) => typeof v === 'boolean').map((v) => (d.flags?.sensitive ? String(v) : anyCase(v)));
      return this.mark(
        { anyOf: [{ enum: only }, ...(spelled.length ? [{ type: 'string', pattern: `^(${spelled.join('|')})$` }] : [])] },
        'converts-string'
      );
    }
    return this.mark({ type: [type, 'string'], pattern: CONVERTED_STRINGS[type] }, 'converts-string');
  }

  protected convert(d?: JoiDescription): JsonSchema {
    if (!d) {
      return {};
    }

    this.ancestors.push(d);

    try {
      const s = this.convertDescription(d);
      const placeholder = this.pendingTargets.get(d);
      if (placeholder && typeof s === 'object') {
        this.linkTargets.set(s, placeholder);
      }
      return s;
    } finally {
      this.ancestors.pop();
    }
  }

  protected convertDescription(d: JoiDescription): JsonSchema {
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
        // Code is a string in YAML, or a list of them (`levels`, `add_group_by`); `values` also takes
        // numbers and booleans, alone or in a list. Core also compiles an object under a key, but an
        // object in a list stays an object.
        s = this.mark(
          {
            type: ['string', 'number', 'boolean', 'array', ...(this.inListItem ? [] : ['object'])],
            items: { type: ['string', 'number', 'boolean'] },
          },
          'function',
        );
        break;
      case 'array':
        s = this.array(d);
        break;
      case 'object':
        if (d.keys) {
          const outer = this.inListItem;
          this.inListItem = false;
          s = this.object(d);
          this.inListItem = outer;
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
        s = this.link(d);
        break;
      default:
        s = this.mark({}, `unknown-type-${d.type}`);
    }

    for (const rule of d.rules ?? []) {
      const limit = rule.args?.limit;
      if (rule.name === 'pattern' && rule.args?.regex) {
        // A JSON Schema `pattern` has no flags and can't be inverted, so such a one is left out.
        const [source, flags] = regexParts(rule.args.regex);
        if (flags || rule.args.options?.invert) {
          this.mark(s, 'pattern');
        } else {
          s.pattern = source;
        }
      } else if (rule.name === 'custom') {
        this.mark(s, 'custom');
      } else if (['length', 'min', 'max'].includes(rule.name)) {
        // Joi counts a string's UTF-16 units and JSON Schema its code points, so only lists get limits.
        if (d.type !== 'array' || typeof limit !== 'number') {
          this.mark(s, 'limit');
        } else {
          if (rule.name !== 'max') {
            s.minItems = limit;
          }
          if (rule.name !== 'min') {
            s.maxItems = limit;
          }
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

  // `Joi.link('...')` counts up through the schemas around it; `Joi.link('#id')` names one by id.
  protected link(d: JoiDescription): JsonSchemaObject {
    const ref = d.link?.ref;
    let target: JoiDescription | undefined;
    if (ref?.type === 'local') {
      target = this.byId(ref.path[0]);
    } else if (ref && !ref.path.length) {
      target = this.ancestors[this.ancestors.length - 1 - (ref.ancestor ?? 0)];
    }
    return target ? { $ref: `#/definitions/${this.placeholder(target)}` } : this.mark({}, 'link');
  }

  protected byId(id: string): JoiDescription | undefined {
    const ancestor = [...this.ancestors].reverse().find((a) => a.flags?.id === id);
    if (ancestor) {
      return ancestor;
    }

    const shared = this.ancestors.flatMap((a) => a.shared ?? []).find((s) => s.flags?.id === id);
    if (shared && !(id in this.sharedSchemas)) {
      this.placeholder(shared);
      this.sharedSchemas[id] = this.convert(shared);
    }
    return shared;
  }

  protected placeholder(target: JoiDescription): string {
    const content = JSON.stringify(target);
    const placeholder = this.placeholders.get(content) ?? `@link${this.placeholders.size}`;
    this.placeholders.set(content, placeholder);
    this.pendingTargets.set(target, placeholder);
    return placeholder;
  }

  protected array(d: JoiDescription): JsonSchemaObject {
    const outer = this.inListItem;
    this.inListItem = true;
    const items = (d.items ?? []).map((item) => this.convert(item));
    this.inListItem = outer;
    if (items.length > 1) {
      return { type: 'array', items: { anyOf: items } };
    }
    return items.length ? { type: 'array', items: items[0] } : { type: 'array' };
  }

  // `Joi.alternatives().try(…)`, `.conditional(Joi.ref('.type'), …)` and `condition(fn, …)`.
  protected alternatives(d: JoiDescription): JsonSchemaObject {
    const s = this.chain(d.matches ?? []);
    if (typeof s === 'boolean') {
      return s ? {} : { not: {} };
    }
    return this.listOfEither(s);
  }

  // A list of one kind of item or a list of another becomes one list of either, which editors can
  // complete; it also takes a list that mixes both, which Core rejects.
  protected listOfEither(s: JsonSchemaObject): JsonSchemaObject {
    const lists = (s.anyOf ?? []).filter((b): b is JsonSchemaObject => (
      typeof b === 'object' && b.type === 'array' && b.items !== undefined && Object.keys(b).length === 2
    ));
    if (!s.anyOf || Object.keys(s).length !== 1 || lists.length !== s.anyOf.length) {
      return s;
    }
    return this.mark({ type: 'array', items: anyOf(lists.map((l) => l.items as JsonSchema)) }, 'list-of-either');
  }

  // Joi tries the matches in order. A condition that fails with no `otherwise` falls through to
  // the next match, and a value no match takes is rejected.
  protected chain(matches: JoiMatch[]): JsonSchema {
    const [m, ...rest] = matches;
    if (!m) {
      return false;
    }
    if (m.schema) {
      const option = this.convert(m.schema);
      return rest.length ? anyOf([option, this.chain(rest)]) : option;
    }
    const cases = m.switch ?? (m.is ? [{ is: m.is, then: m.then, otherwise: m.otherwise }] : []);
    return this.cases(m.ref, cases, rest);
  }

  protected cases(ref: JoiRef | undefined, cases: JoiCase[], rest: JoiMatch[]): JsonSchema {
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
      if (then === false || otherwise === false) {
        return anyOf([then, otherwise]);
      }
      // The condition is a JS function: both branches are kept, the choice between them is lost.
      return this.mark({ anyOf: [then, otherwise] }, 'function-condition');
    }
    return { if: condition, then, else: otherwise };
  }

  // The `if` for a Joi condition, or undefined when the condition can't be said exactly: an `if`
  // looser than Joi's would apply `then` to values Joi sends elsewhere.
  protected condition(ref: JoiRef | undefined, is: JoiDescription): JsonSchema | undefined {
    if (is.rules?.some((r) => r.name === 'custom')) {
      return undefined;
    }
    if (!ref || (ref.ancestor === 0 && !ref.path.length)) {
      return this.convert(is);
    }
    const key = ownKey(ref);
    return key ? keyMatches(key, valuesFor(key, this.convert(is)), is.flags?.presence === 'required') : undefined;
  }

  protected object(d: JoiDescription): JsonSchemaObject {
    const properties: Record<string, JsonSchema> = {};
    const patternProperties: Record<string, JsonSchema> = {};
    const required: string[] = [];
    const allOf: JsonSchema[] = [];

    for (const [key, value] of Object.entries(d.keys ?? {})) {
      const name = snake(key);
      const overwritten = OVERWRITTEN_KEYS.includes(key);
      const hoisted = !overwritten && value.whens?.every(isExactWhen) ? value.whens : undefined;
      const property = overwritten ? {} : valuesFor(key, this.convert(hoisted ? { ...value, whens: undefined } : value));
      if (overwritten || NOT_OFFERED_KEYS.includes(key)) {
        patternProperties[exactly(name)] = property;
        patternProperties[exactly(key)] = property;
      } else {
        properties[name] = property;
        if (key !== name) {
          patternProperties[exactly(key)] = deprecated(property);
        }
      }

      for (const w of hoisted ?? []) {
        allOf.push(...this.whenCases(key, w, hoisted?.length === 1));
      }
      if (value.flags?.presence === 'required' && !overwritten) {
        if (key === name) {
          required.push(name);
        } else {
          allOf.push({ anyOf: [{ required: [name] }, { required: [key] }] });
        }
      }
    }

    const s: JsonSchemaObject = { type: 'object', properties };
    if (Object.keys(patternProperties).length) {
      s.patternProperties = patternProperties;
    }
    s.additionalProperties = false;
    if (required.length) {
      s.required = required;
    }

    allOf.push(...(d.dependencies ?? []).flatMap((dep) => dependency(dep, 'extends' in (d.keys ?? {}))));
    if (allOf.length) {
      s.allOf = allOf;
    }
    return s;
  }

  // `field: Joi.when('type', …)` becomes an `if` on the sibling `type` at the parent object.
  protected whenCases(key: string, w: JoiWhen, demandsKey: boolean): JsonSchemaObject[] {
    const sibling = siblingKey(w.ref) as string;
    const cases = whenCasesOf(w);
    const out: JsonSchemaObject[] = cases.map((c) => ({
      if: keyIn(sibling, values(c.is.allow), c.is.flags?.presence === 'required'),
      then: this.whenBranch(key, c.then, demandsKey),
    }));

    const otherwise = otherwiseOf(w);
    if (otherwise) {
      const covered = cases.flatMap((c) => values(c.is.allow));
      const takesMissing = cases.some((c) => c.is.flags?.presence !== 'required');
      out.push({ if: { not: keyIn(sibling, covered, !takesMissing) }, then: this.whenBranch(key, otherwise, demandsKey) });
    }
    return out;
  }

  // `convert` leaves a key's `.required()` to its object, so a branch demands the key itself; not with
  // a second `when`, whose match Joi merges in and may make the key optional again.
  protected whenBranch(key: string, d: JoiDescription | undefined, demandsKey: boolean): JsonSchemaObject {
    const branch = inEitherSpelling(key, this.convert(d));
    return demandsKey && d?.flags?.presence === 'required' ? { ...branch, ...present(key) } : branch;
  }
}

export function modelSchemaFromJoi(described: ModelSchemaDescriptions) {
  const converter = new ModelSchemaConverter();
  const inline = converter.modelSchema(described);
  return { schema: withDefinitions(inline, converter.linkTargets, converter.sharedSchemas), stats: converter.stats };
}
