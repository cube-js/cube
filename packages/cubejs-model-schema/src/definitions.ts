// Gives the manifest its `definitions`: each model entity, each `Joi.link` target and each part
// the manifest repeats is written there once and referred to with `$ref`.
import type { JsonSchema, JsonSchemaObject } from './converter';

// A repeat shorter than this stays inline: its `$ref` would save little.
const MIN_SHARED_LENGTH = 200;

// Lists whose items are the model's entities; those get a definition even when used once.
const ENTITY_LISTS = [
  'cubes', 'views', 'view_groups', 'measures', 'dimensions', 'segments', 'pre_aggregations', 'hierarchies',
  'joins', 'folders', 'access_policy',
];

const SINGULAR: Record<string, string> = { aliases: 'alias', hierarchies: 'hierarchy', indexes: 'index' };

const singular = (key: string) => SINGULAR[key] ?? key.replace(/s$/, '');

const snake = (key: string) => key.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`);

// A property name, or a keyword such as `items` or `anyOf`, on the way from the root to a schema.
type Step = { key: string } | { keyword: string };

const SUBSCHEMA_KEYWORDS = ['items', 'not', 'if', 'then', 'else'] as const;

const LIST_KEYWORDS = ['anyOf', 'allOf'] as const;

// Every subschema of `s`, in the order both passes visit them.
function subschemas(s: JsonSchemaObject): [Step, JsonSchema][] {
  return [
    ...Object.entries(s.properties ?? {}).map(([key, c]): [Step, JsonSchema] => [{ key }, c]),
    // A camelCase spelling, `^dataSource$`, is named like its snake_case one.
    ...Object.entries(s.patternProperties ?? {}).map(([key, c]): [Step, JsonSchema] => [{ key: snake(key.slice(1, -1)) }, c]),
    ...SUBSCHEMA_KEYWORDS.flatMap((keyword): [Step, JsonSchema][] => (s[keyword] === undefined ? [] : [[{ keyword }, s[keyword] as JsonSchema]])),
    ...LIST_KEYWORDS.flatMap((keyword) => (s[keyword] ?? []).map((c): [Step, JsonSchema] => [{ keyword }, c])),
  ];
}

// What tells a branch of a property's schema from the others: the field and value of a hoisted
// `when` (`format_type_number`), the value its `if` tests, or its first key.
function trait(s: JsonSchemaObject): string {
  const field = typeof s.then === 'object' && !s.then.type ? Object.keys(s.then.properties ?? {})[0] : undefined;
  const condition = typeof s.if === 'object' ? s.if : undefined;
  const negated = typeof condition?.not === 'object';
  const tested = negated ? condition?.not : condition;
  const test = typeof tested === 'object' ? Object.entries(tested.properties ?? {})[0] : undefined;
  const value = test && typeof test[1] === 'object' ? test[1].enum?.[0] : undefined;
  const when = test && typeof value === 'string' ? `${negated ? 'not_' : ''}${test[0]}_${snake(value)}` : undefined;
  return [field, when].filter(Boolean).join('_') || Object.keys(s.properties ?? {}).find((key) => key !== 'name') || 'variant';
}

// Names for `s` at `path`, best first: the property it describes (with the branch's trait when it
// is one branch of it), then that prefixed with the properties around it.
function names(s: JsonSchemaObject, path: Step[]): string[] {
  const words = path.flatMap((step, i) => {
    if (!('key' in step)) {
      return [];
    }
    const next = path[i + 1];
    return [next && 'keyword' in next && next.keyword === 'items' ? singular(step.key) : step.key];
  });
  const last = path.map((step) => 'key' in step).lastIndexOf(true);
  const branch = path.slice(last + 1).some((step) => 'keyword' in step && step.keyword !== 'items');
  const base = `${words[words.length - 1] ?? 'schema'}${branch ? `_${trait(s)}` : ''}`;
  return words.slice(0, -1).reverse().reduce((acc, word) => [...acc, `${word}_${acc[acc.length - 1]}`], [base]);
}

// json-schema-library 9, which codemirror-json-schema completes with, doesn't resolve a `$ref` that
// is a whole `items` or `patternProperties` value until it has data to match; inside `allOf` it does.
const wrapped = (s: JsonSchema): JsonSchema => (typeof s === 'object' && s.$ref ? { allOf: [s] } : s);

class DefinitionWriter {
  public readonly definitions: Record<string, JsonSchema> = {};

  // Each placeholder a link target's `$ref`s carry, with the name the target got.
  public readonly names = new Map<string, string>();

  protected readonly repeated = new Set<string>();

  protected readonly byContent = new Map<string, string>();

  public constructor(roots: JsonSchema[], protected readonly linkTargets: Map<JsonSchemaObject, string>) {
    const seen = new Set<string>();
    // A part inside a repeated part is counted once: it repeats only if it also appears elsewhere.
    const count = (s: JsonSchema) => {
      if (typeof s === 'object') {
        const content = JSON.stringify(s);
        if (seen.has(content)) {
          this.repeated.add(content);
        } else {
          seen.add(content);
          subschemas(s).forEach(([, c]) => count(c));
        }
      }
    };
    roots.forEach(count);
  }

  public write(s: JsonSchema, path: Step[]): JsonSchema {
    if (typeof s !== 'object') {
      return s;
    }

    const content = JSON.stringify(s);
    const known = this.byContent.get(content);
    if (known) {
      return { $ref: `#/definitions/${known}` };
    }

    if (path.length && (this.isEntity(s, path) || this.linkTargets.has(s))) {
      const name = this.freeName(s, path);
      // Taken before the parts inside are written, so none of them gets the same name.
      this.definitions[name] = true;
      this.byContent.set(content, name);
      const placeholder = this.linkTargets.get(s);
      if (placeholder) {
        this.names.set(placeholder, name);
      }
      this.definitions[name] = this.rebuild(s, path);
      return { $ref: `#/definitions/${name}` };
    }

    const out = this.rebuild(s, path);
    // A repeat is measured with its own repeats already replaced by `$ref`s.
    if (path.length && this.repeated.has(content) && JSON.stringify(out).length >= MIN_SHARED_LENGTH) {
      const name = this.freeName(s, path);
      this.byContent.set(content, name);
      this.definitions[name] = out;
      return { $ref: `#/definitions/${name}` };
    }
    return out;
  }

  // An item of an entity list; a list of member names, such as a pre-aggregation's `measures`, has
  // a scalar `type`.
  protected isEntity(s: JsonSchemaObject, path: Step[]): boolean {
    const [list, items] = path.slice(-2);
    return path.length >= 2 && 'key' in list && ENTITY_LISTS.includes(list.key) && 'keyword' in items
      && items.keyword === 'items' && (s.type === undefined || s.type === 'object');
  }

  protected freeName(s: JsonSchemaObject, path: Step[]): string {
    const candidates = names(s, path);
    const free = candidates.find((name) => !(name in this.definitions));
    if (free) {
      return free;
    }

    let n = 2;
    while (`${candidates[0]}_${n}` in this.definitions) {
      n += 1;
    }
    return `${candidates[0]}_${n}`;
  }

  protected rebuild(s: JsonSchemaObject, path: Step[]): JsonSchemaObject {
    const out: JsonSchemaObject = { ...s };
    if (s.properties) {
      out.properties = Object.fromEntries(Object.entries(s.properties).map(([key, c]) => [key, this.write(c, [...path, { key }])]));
    }
    if (s.patternProperties) {
      out.patternProperties = Object.fromEntries(Object.entries(s.patternProperties).map(([key, c]) => [
        key, wrapped(this.write(c, [...path, { key: snake(key.slice(1, -1)) }])),
      ]));
    }

    for (const keyword of SUBSCHEMA_KEYWORDS) {
      if (s[keyword] !== undefined) {
        const c = this.write(s[keyword] as JsonSchema, [...path, { keyword }]);
        out[keyword] = keyword === 'items' ? wrapped(c) : c;
      }
    }

    for (const keyword of LIST_KEYWORDS) {
      if (s[keyword]) {
        out[keyword] = (s[keyword] as JsonSchema[]).map((c) => this.write(c, [...path, { keyword }]));
      }
    }
    return out;
  }
}

// `shared` holds link targets that are not in the tree, by their Joi id (`nestedViewGroupSchema`).
export function withDefinitions(
  root: JsonSchemaObject,
  linkTargets: Map<JsonSchemaObject, string>,
  shared: Record<string, JsonSchema>,
): JsonSchemaObject {
  const sharedRoot: JsonSchemaObject = {
    properties: Object.fromEntries(Object.entries(shared).map(([id, s]) => [snake(id.replace(/Schema$/, '')), s])),
  };
  const writer = new DefinitionWriter([root, sharedRoot], linkTargets);
  const out = writer.write(root, []) as JsonSchemaObject;
  writer.write(sharedRoot, []);

  const definitions = Object.fromEntries(Object.entries(writer.definitions).sort(([a], [b]) => (a < b ? -1 : 1)));
  const json = JSON.stringify({ ...out, definitions }).replace(/#\/definitions\/(@link\d+)"/g, (ref, placeholder) => {
    const name = writer.names.get(placeholder);
    if (!name) {
      throw new Error(`The target of the link ${placeholder} got no definition`);
    }
    return `#/definitions/${name}"`;
  });
  return JSON.parse(json);
}
