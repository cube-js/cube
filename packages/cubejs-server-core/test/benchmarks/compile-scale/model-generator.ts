import {
  BOOLEAN_ATTRS,
  DOMAINS,
  DomainSpec,
  EntitySpec,
  NUMBER_ATTRS,
  STRING_ATTRS,
  TIME_ATTRS,
} from './domains';

/**
 * Synthetic data model for compile-time benchmarks: 100 distinct cubes (10 domains x 10 entities)
 * with exactly 100 members each (dimensions + measures + segments), multiplied `copies` times.
 *
 * Member SQL is written in the YAML reference syntax: `{CUBE}`, `{member}`, `{cube.member}`.
 * A cube reference carries a `@` marker (`{@customers.status}`) that each emitter replaces with
 * the name of that cube within the same copy, so copy 3 of `orders` joins copy 3 of `customers`.
 */

export const MEMBERS_PER_CUBE = 100;

export const BASE_CUBES = DOMAINS.reduce((n, d) => n + d.entities.length, 0);

export type ModelFormat = 'jinja' | 'yaml' | 'js';

export const MODEL_FORMATS: ModelFormat[] = ['jinja', 'yaml', 'js'];

type Dimension = {
  name: string;
  type: 'string' | 'number' | 'time' | 'boolean';
  sql?: string;
  primaryKey?: boolean;
  case?: { when: { sql: string; label: string }[]; else: string };
  granularities?: { name: string; interval: string; offset?: string; origin?: string }[];
};

type Measure = {
  name: string;
  type: string;
  sql?: string;
  filters?: string[];
  rollingWindow?: string;
  format?: string;
};

type Segment = { name: string; sql: string };

type Join = { target: string; sql: string };

type PreAggregation = {
  name: string;
  measures: string[];
  dimensions: string[];
  timeDimension: string;
  granularity: string;
  partitionGranularity: string;
};

type CubeSpec = {
  domain: string;
  name: string;
  sqlTable: string;
  joins: Join[];
  dimensions: Dimension[];
  measures: Measure[];
  segments: Segment[];
  preAggregations: PreAggregation[];
};

type ViewSpec = {
  name: string;
  joinPaths: { path: string[]; includes: '*' | string[]; prefix: boolean }[];
};

const window = <T>(pool: T[], offset: number, size: number): T[] => {
  if (size > pool.length) {
    throw new Error(`Window of ${size} does not fit a pool of ${pool.length}`);
  }

  return Array.from({ length: size }, (_, i) => pool[(offset + i) % pool.length]);
};

const singular = (name: string) => name.replace(/ies$/, 'y').replace(/(s|es)$/, '');

const fkName = (ref: string) => `${singular(ref)}_id`;

function buildCube(domain: DomainSpec, entity: EntitySpec, index: number): CubeSpec {
  const isFact = domain.entities[0] === entity;
  const strings = window(STRING_ATTRS, index * 7, 40);
  const numbers = window(NUMBER_ATTRS, index * 3, 12);
  const times = ['created_at', ...window(TIME_ATTRS.filter((t) => t !== 'created_at'), index, 5)];
  const booleans = window(BOOLEAN_ATTRS, index, 5);
  const [amount, quantity] = numbers;

  const dimensions: Dimension[] = [
    { name: 'id', type: 'number', sql: '{CUBE}.id', primaryKey: true },
    ...entity.refs.map((ref): Dimension => ({ name: fkName(ref), type: 'number', sql: `{CUBE}.${fkName(ref)}` })),
    ...times.map((t, i): Dimension => ({
      name: t,
      type: 'time',
      sql: `{CUBE}.${t}`,
      ...(isFact && i === 0 && {
        granularities: [
          { name: 'fiscal_quarter', interval: '3 months', offset: '1 month' },
          { name: 'two_weeks', interval: '2 weeks', origin: '2024-01-01' },
        ],
      }),
    })),
    ...booleans.map((b): Dimension => ({ name: b, type: 'boolean', sql: `{CUBE}.${b}` })),
    ...numbers.map((n): Dimension => ({ name: n, type: 'number', sql: `{CUBE}.${n}` })),
    // Derived dimensions reference other members of the same cube
    { name: `${amount}_bucket`, type: 'string', sql: `CASE WHEN {${amount}} < 100 THEN 'small' WHEN {${amount}} < 1000 THEN 'medium' ELSE 'large' END` },
    { name: 'created_year', type: 'number', sql: 'EXTRACT(YEAR FROM {created_at})' },
    { name: 'age_days', type: 'number', sql: 'DATE_PART(\'day\', NOW() - {created_at})' },
    { name: `is_large_${amount}`, type: 'boolean', sql: `{${amount}} > 1000` },
    { name: `${strings[0]}_${strings[1]}`, type: 'string', sql: `{${strings[0]}} || ' / ' || {${strings[1]}}` },
    {
      name: `${strings[2]}_group`,
      type: 'string',
      case: {
        when: [
          { sql: `{CUBE}.${strings[2]} IN ('a', 'b')`, label: 'primary' },
          { sql: `{CUBE}.${strings[2]} IN ('c', 'd')`, label: 'secondary' },
        ],
        else: 'other',
      },
    },
    // Dimensions pulled from the joined cubes
    ...entity.refs.map((ref): Dimension => ({
      name: `${singular(ref)}_label`,
      type: 'string',
      sql: `{@${ref}.id} || ''`,
    })),
  ];

  const dimensionCount = 60;
  const stringSlots = dimensionCount - dimensions.length;
  dimensions.push(...strings.slice(3, 3 + stringSlots).map((s): Dimension => ({ name: s, type: 'string', sql: `{CUBE}.${s}` })));
  dimensions.splice(2, 0, ...strings.slice(0, 3).map((s): Dimension => ({ name: s, type: 'string', sql: `{CUBE}.${s}` })));
  // The three strings the derived dimensions reference displaced three generic ones
  dimensions.splice(dimensionCount);

  const measures: Measure[] = [
    { name: 'count', type: 'count' },
    ...entity.refs.map((ref): Measure => ({ name: `${singular(ref)}_count`, type: 'count_distinct', sql: `{${fkName(ref)}}` })),
    ...numbers.slice(0, 8).map((n): Measure => ({ name: `total_${n}`, type: 'sum', sql: `{${n}}`, format: 'number' })),
    ...numbers.slice(0, 6).map((n): Measure => ({ name: `avg_${n}`, type: 'avg', sql: `{${n}}` })),
    ...numbers.slice(0, 3).map((n): Measure => ({ name: `min_${n}`, type: 'min', sql: `{${n}}` })),
    ...numbers.slice(0, 3).map((n): Measure => ({ name: `max_${n}`, type: 'max', sql: `{${n}}` })),
    ...booleans.slice(0, 4).map((b): Measure => ({ name: `${b.replace(/^is_/, '')}_count`, type: 'count', filters: [`{${b}}`] })),
    { name: `${booleans[0].replace(/^is_/, '')}_${amount}`, type: 'sum', sql: `{${amount}}`, filters: [`{${booleans[0]}}`] },
    { name: 'approx_unique', type: 'count_distinct_approx', sql: '{id}' },
    // Calculated measures reference other measures
    { name: `${amount}_per_record`, type: 'number', sql: `{total_${amount}} / NULLIF({count}, 0)` },
    { name: `${quantity}_per_record`, type: 'number', sql: `{total_${quantity}} / NULLIF({count}, 0)` },
    { name: `${booleans[0].replace(/^is_/, '')}_share`, type: 'number', sql: `1.0 * {${booleans[0].replace(/^is_/, '')}_count} / NULLIF({count}, 0)`, format: 'percent' },
    { name: `rolling_${amount}_7d`, type: 'sum', sql: `{${amount}}`, rollingWindow: '7 day' },
    { name: 'rolling_count_30d', type: 'count', rollingWindow: '30 day' },
  ];

  const measureCount = 35;
  let extra = 8;
  while (measures.length < measureCount) {
    const n = numbers[extra % numbers.length];
    measures.push({ name: `total_${n}`, type: 'sum', sql: `{${n}}` });
    extra += 1;
  }
  if (measures.length !== measureCount) {
    throw new Error(`${entity.name}: ${measures.length} measures, expected ${measureCount}`);
  }

  const segments: Segment[] = booleans.map((b) => ({ name: `${b.replace(/^is_/, '')}_only`, sql: `{CUBE}.${b} = TRUE` }));

  const cube: CubeSpec = {
    domain: domain.name,
    name: entity.name,
    sqlTable: `${domain.name}.${entity.name}`,
    joins: entity.refs.map((ref) => ({ target: ref, sql: `{CUBE}.${fkName(ref)} = {@${ref}.id}` })),
    dimensions,
    measures,
    segments,
    preAggregations: isFact ? [{
      name: 'daily',
      measures: ['count', `total_${amount}`, `total_${quantity}`],
      dimensions: [strings[3], booleans[0]],
      timeDimension: 'created_at',
      granularity: 'day',
      partitionGranularity: 'month',
    }] : [],
  };

  const members = cube.dimensions.length + cube.measures.length + cube.segments.length;
  const names = new Set([...cube.dimensions, ...cube.measures, ...cube.segments].map((m) => m.name));
  if (members !== MEMBERS_PER_CUBE || names.size !== members) {
    throw new Error(`${entity.name}: ${members} members (${names.size} unique), expected ${MEMBERS_PER_CUBE}`);
  }

  return cube;
}

function buildView(domain: DomainSpec, cubes: Map<string, CubeSpec>): ViewSpec {
  const fact = domain.entities[0];

  return {
    name: `${domain.name}_overview`,
    joinPaths: [
      { path: [fact.name], includes: '*', prefix: false },
      ...fact.refs.map((ref) => ({
        path: [fact.name, ref],
        includes: cubes.get(ref)!.dimensions.slice(0, 8).map((d) => d.name)
          .concat(cubes.get(ref)!.measures.slice(0, 4).map((m) => m.name)),
        prefix: true,
      })),
    ],
  };
}

export type BaseModel = { cubes: CubeSpec[]; views: Map<string, ViewSpec> };

export function buildBaseModel(): BaseModel {
  const cubes: CubeSpec[] = [];
  const views = new Map<string, ViewSpec>();
  let index = 0;

  for (const domain of DOMAINS) {
    const domainCubes = new Map<string, CubeSpec>();

    for (const entity of domain.entities) {
      const cube = buildCube(domain, entity, index++);
      cubes.push(cube);
      domainCubes.set(entity.name, cube);
    }
    // The view lives in the fact cube's file
    views.set(domain.entities[0].name, buildView(domain, domainCubes));
  }

  return { cubes, views };
}

// ---------------------------------------------------------------------------------------------
// Members as they are written in a model file, and the half of them loaded at compile time

type Plain = string | number | boolean | Plain[] | { [key: string]: Plain };
type PlainMember = { name: string } & { [key: string]: Plain };
type MemberKind = 'dimensions' | 'measures' | 'segments';

const MEMBER_KINDS: MemberKind[] = ['dimensions', 'measures', 'segments'];

function dimensionToPlain(d: Dimension): PlainMember {
  return {
    name: d.name,
    type: d.type,
    ...(d.sql && { sql: d.sql }),
    ...(d.primaryKey && { primary_key: true }),
    ...(d.case && { case: { when: d.case.when.map((w) => ({ sql: w.sql, label: w.label })), else: { label: d.case.else } } }),
    ...(d.granularities && { granularities: d.granularities.map((g) => ({ ...g })) }),
  };
}

function measureToPlain(m: Measure): PlainMember {
  return {
    name: m.name,
    type: m.type,
    ...(m.sql && { sql: m.sql }),
    ...(m.format && { format: m.format }),
    ...(m.filters && { filters: m.filters.map((sql) => ({ sql })) }),
    ...(m.rollingWindow && { rolling_window: { trailing: m.rollingWindow } }),
  };
}

function plainMembers(cube: CubeSpec): Record<MemberKind, PlainMember[]> {
  return {
    dimensions: cube.dimensions.map(dimensionToPlain),
    measures: cube.measures.map(measureToPlain),
    segments: cube.segments.map((s) => ({ name: s.name, sql: s.sql })),
  };
}

/** Members kept in the model file; the rest of each list is loaded at compile time */
const STATIC_MEMBERS: Record<MemberKind, number> = { dimensions: 30, measures: 18, segments: 2 };

export const STATIC_MEMBERS_PER_CUBE = Object.values(STATIC_MEMBERS).reduce((a, b) => a + b, 0);

function splitMembers(cube: CubeSpec) {
  const all = plainMembers(cube);
  const pick = (from: 'static' | 'dynamic') => Object.fromEntries(MEMBER_KINDS.map((kind) => [
    kind,
    from === 'static' ? all[kind].slice(0, STATIC_MEMBERS[kind]) : all[kind].slice(STATIC_MEMBERS[kind]),
  ])) as Record<MemberKind, PlainMember[]>;

  return { all, static: pick('static'), dynamic: pick('dynamic') };
}

export type MemberApi = {
  revision: number;
  cubes: Record<string, Record<MemberKind, PlainMember[]>>;
};

/**
 * What the model loads at compile time: the dynamic half of every base cube, keyed by
 * `domain.entity`, as a remote API or a manifest would serve it. The revision shows up in every
 * member's description, so a compile shows which revision it loaded.
 */
export function buildMemberApi(revision: number): MemberApi {
  const { cubes } = buildBaseModel();

  return {
    revision,
    cubes: Object.fromEntries(cubes.map((cube) => [
      `${cube.domain}.${cube.name}`,
      Object.fromEntries(Object.entries(splitMembers(cube).dynamic).map(([kind, members]) => [
        kind,
        members.map((m) => ({ ...m, description: `Loaded from the member API, revision ${revision}` })),
      ])) as unknown as Record<MemberKind, PlainMember[]>,
    ])),
  };
}

// ---------------------------------------------------------------------------------------------
// Emitters

/** `name` of a cube or view within one copy; `suffix` is `_3` or `_{{ i }}` */
type Namer = (name: string) => string;

const REF = /\{@(\w+)/g;

const resolveRefs = (sql: string, name: Namer) => sql.replace(REF, (_, cube) => `{${name(cube)}`);

const resolvePlain = (value: Plain, name: Namer): Plain => {
  if (typeof value === 'string') {
    return resolveRefs(value, name);
  } else if (Array.isArray(value)) {
    return value.map((v) => resolvePlain(v, name));
  } else if (typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, resolvePlain(v, name)]));
  }

  return value;
};

const YAML_KEYWORDS = new Set(['true', 'false', 'yes', 'no', 'on', 'off', 'null']);

const yamlScalar = (v: string | number | boolean) => (
  typeof v === 'string' && (!/^[A-Za-z_]\w*$/.test(v) || YAML_KEYWORDS.has(v.toLowerCase())) ? JSON.stringify(v) : String(v)
);

/** Block-style YAML for a list item, the way a model file is written by hand */
function yamlItem(value: { [key: string]: Plain }, indent: string): string[] {
  const lines: string[] = [];
  const write = (v: Plain, pad: string, prefix: string) => {
    if (Array.isArray(v)) {
      lines.push(`${prefix}`);
      v.forEach((item) => {
        if (typeof item === 'object' && !Array.isArray(item)) {
          const nested = yamlItem(item, `${pad}  `);
          lines.push(...nested);
        } else {
          lines.push(`${pad}  - ${yamlScalar(item as string)}`);
        }
      });
    } else if (typeof v === 'object') {
      lines.push(`${prefix}`);
      Object.entries(v).forEach(([k, inner]) => write(inner, `${pad}  `, `${pad}  ${k}:`));
    } else {
      lines.push(`${prefix} ${yamlScalar(v)}`);
    }
  };

  Object.entries(value).forEach(([k, v], i) => write(v, `${indent}  `, `${indent}${i === 0 ? '- ' : '  '}${k}:`));
  return lines;
}

type Loader = (kind: MemberKind) => string[];

function cubeToYaml(
  cube: CubeSpec,
  members: Record<MemberKind, PlainMember[]>,
  name: Namer,
  loadDynamic?: Loader,
): string[] {
  const indent = '  ';
  const lines = [
    `${indent}- name: ${name(cube.name)}`,
    `${indent}  sql_table: ${cube.sqlTable}`,
  ];
  if (cube.joins.length) {
    lines.push(`${indent}  joins:`);

    for (const join of cube.joins) {
      lines.push(...yamlItem({ name: name(join.target), relationship: 'many_to_one', sql: resolveRefs(join.sql, name) }, `${indent}    `));
    }
  }

  for (const kind of MEMBER_KINDS) {
    lines.push(`${indent}  ${kind}:`);
    members[kind].forEach((m) => lines.push(...yamlItem(resolvePlain(m, name) as PlainMember, `${indent}    `)));
    if (loadDynamic) {
      lines.push(...loadDynamic(kind));
    }
  }
  if (cube.preAggregations.length) {
    lines.push(`${indent}  pre_aggregations:`);

    for (const p of cube.preAggregations) {
      lines.push(
        `${indent}    - name: ${p.name}`,
        `${indent}      measures: [${p.measures.join(', ')}]`,
        `${indent}      dimensions: [${p.dimensions.join(', ')}]`,
        `${indent}      time_dimension: ${p.timeDimension}`,
        `${indent}      granularity: ${p.granularity}`,
        `${indent}      partition_granularity: ${p.partitionGranularity}`,
      );
    }
  }

  return lines;
}

function viewToYaml(view: ViewSpec, name: Namer): string[] {
  const lines = [`  - name: ${name(view.name)}`, '    cubes:'];

  for (const jp of view.joinPaths) {
    lines.push(`      - join_path: ${jp.path.map(name).join('.')}`);
    lines.push(jp.includes === '*' ? '        includes: "*"' : `        includes: [${jp.includes.join(', ')}]`);
    if (jp.prefix) {
      lines.push('        prefix: true');
    }
  }

  return lines;
}

const jsTemplate = (sql: string) => `\`${sql.replace(/`/g, '\\`').replace(/\{([\w.]+)\}/g, (_, ref) => `\${${ref}}`)}\``;

/** YAML lists granularities; JS keys them by name */
const namedToObject = (items: Plain[]): Plain => Object.fromEntries(items.map((item) => {
  const { name, ...rest } = item as { [key: string]: Plain };
  return [name as string, rest];
}));

/** A plain member as a JS object literal, with every `sql` turned into a template literal */
function jsValue(value: Plain, key?: string): string {
  if (typeof value === 'string') {
    return key === 'sql' ? jsTemplate(value) : JSON.stringify(value);
  } else if (Array.isArray(value) && key === 'granularities') {
    return jsValue(namedToObject(value));
  } else if (Array.isArray(value)) {
    return `[${value.map((v) => jsValue(v)).join(', ')}]`;
  } else if (typeof value === 'object') {
    return `{ ${Object.entries(value).map(([k, v]) => `${k}: ${jsValue(v, k)}`).join(', ')} }`;
  }

  return String(value);
}

function cubeToJs(cube: CubeSpec, members: Record<MemberKind, PlainMember[]>, name: Namer, loadDynamic?: (kind: MemberKind) => string): string {
  const lines = [`cube(\`${name(cube.name)}\`, {`, `  sql_table: \`${cube.sqlTable}\`,`];
  if (cube.joins.length) {
    lines.push('  joins: {');

    for (const join of cube.joins) {
      lines.push(`    ${name(join.target)}: { relationship: \`many_to_one\`, sql: ${jsTemplate(resolveRefs(join.sql, name))} },`);
    }
    lines.push('  },');
  }

  for (const kind of MEMBER_KINDS) {
    lines.push(`  ${kind}: {`);

    for (const { name: memberName, ...m } of members[kind]) {
      lines.push(`    ${memberName}: ${jsValue(resolvePlain(m, name))},`);
    }
    if (loadDynamic) {
      lines.push(`    ...${loadDynamic(kind)},`);
    }
    lines.push('  },');
  }
  if (cube.preAggregations.length) {
    lines.push('  pre_aggregations: {');

    for (const p of cube.preAggregations) {
      lines.push(
        `    ${p.name}: {`,
        `      measures: [${p.measures.map((m) => `CUBE.${m}`).join(', ')}],`,
        `      dimensions: [${p.dimensions.map((d) => `CUBE.${d}`).join(', ')}],`,
        `      time_dimension: CUBE.${p.timeDimension},`,
        `      granularity: \`${p.granularity}\`,`,
        `      partition_granularity: \`${p.partitionGranularity}\`,`,
        '    },',
      );
    }
    lines.push('  },');
  }
  lines.push('});');

  return lines.join('\n');
}

function viewToJs(view: ViewSpec, name: Namer): string {
  const cubes = view.joinPaths.map((jp) => {
    const includes = jp.includes === '*' ? '`*`' : `[${jp.includes.map((i) => `\`${i}\``).join(', ')}]`;
    return `    { join_path: ${jp.path.map(name).join('.')}, includes: ${includes}${jp.prefix ? ', prefix: true' : ''} },`;
  });

  return [`view(\`${name(view.name)}\`, {`, '  cubes: [', ...cubes, '  ],', '});'].join('\n');
}

/**
 * globals.py for the Jinja model: loads the member API (a JSON file standing in for a remote
 * endpoint) and hands each cube copy its members with cube references resolved to that copy.
 * The file is re-read when it changes, as a live API call would see new data.
 */
function globalsPy(memberApiFile: string): string {
  return `import json
import os
import re

from cube import TemplateContext

template = TemplateContext()

MEMBER_API_FILE = ${JSON.stringify(memberApiFile)}
CUBE_REF = re.compile(r"\\{@(\\w+)")

_loaded = {}


def _member_api():
    mtime = os.stat(MEMBER_API_FILE).st_mtime_ns
    if _loaded.get("mtime") != mtime:
        with open(MEMBER_API_FILE) as f:
            _loaded.update(mtime=mtime, api=json.load(f))
    return _loaded["api"]


def _resolve(value, suffix):
    if isinstance(value, str):
        return CUBE_REF.sub(lambda m: "{" + m.group(1) + suffix, value)
    if isinstance(value, list):
        return [_resolve(v, suffix) for v in value]
    if isinstance(value, dict):
        return {k: _resolve(v, suffix) for k, v in value.items()}
    return value


@template.function("dynamic_members")
def dynamic_members(cube, kind, copy):
    return _resolve(_member_api()["cubes"][cube][kind], f"_{copy}")
`;
}

/**
 * The JS model's counterpart of globals.py. Member SQL must be a function whose parameter names
 * are the referenced cubes and members, so each \`{ref}\` becomes a parameter of a generated
 * function; a plain string would reach the SQL unresolved.
 */
function dynamicMembersJs(memberApiFile: string): string {
  return `const fs = require('fs');

const api = JSON.parse(fs.readFileSync(${JSON.stringify(memberApiFile)}, 'utf8'));

const REF = /\\{(@?)(\\w+)((?:\\.\\w+)*)\\}/g;

function toSqlFunction(sql, suffix) {
  const params = [];
  const body = sql.replace(REF, (_, isCube, head, rest) => {
    const param = isCube ? head + suffix : head;
    if (!params.includes(param)) {
      params.push(param);
    }
    return '\${' + param + rest + '}';
  });
  return new Function(...params, 'return \`' + body.replace(/\`/g, '\\\\\`') + '\`;');
}

function withSqlFunctions(value, suffix) {
  if (Array.isArray(value)) {
    return value.map((v) => withSqlFunctions(v, suffix));
  }
  if (value && typeof value === 'object') {
    const res = {};
    Object.keys(value).forEach((k) => {
      if (k === 'sql') {
        res[k] = toSqlFunction(value[k], suffix);
      } else if (k === 'granularities') {
        // YAML lists granularities; JS keys them by name
        res[k] = {};
        value[k].forEach(({ name, ...g }) => {
          res[k][name] = g;
        });
      } else {
        res[k] = withSqlFunctions(value[k], suffix);
      }
    });
    return res;
  }
  return value;
}

export function dynamicMembers(cube, kind, suffix) {
  const res = {};
  api.cubes[cube][kind].forEach(({ name, ...member }) => {
    res[name] = withSqlFunctions(member, suffix);
  });
  return res;
}
`;
}

export type ModelFile = { fileName: string; content: string };

export type GeneratedModel = {
  files: ModelFile[];
  cubes: number;
  views: number;
  cubeMembers: number;
  dynamicMembers: number;
};

/**
 * `jinja` writes 100 files, each looping `COMPILE_CONTEXT.securityContext.copies` times over its
 * cube, with half of every cube's members loaded at compile time by `globals.py`.
 * `js` writes one file per cube copy, loading the same half through `dynamic_members.js`.
 * `yaml` writes every member statically, one file per cube copy: the reference point.
 */
export function generateModel(
  copies: number,
  format: ModelFormat,
  { views: withViews = true, memberApiFile }: { views?: boolean; memberApiFile: string },
): GeneratedModel {
  const { cubes, views } = buildBaseModel();
  const files: ModelFile[] = [];
  const suffixed = (suffix: string): Namer => (n) => `${n}${suffix}`;

  if (format === 'jinja') {
    files.push({ fileName: 'globals.py', content: globalsPy(memberApiFile) });
  } else if (format === 'js') {
    files.push({ fileName: 'dynamic_members.js', content: dynamicMembersJs(memberApiFile) });
  }

  for (const cube of cubes) {
    const view = withViews ? views.get(cube.name) : undefined;
    const members = splitMembers(cube);
    const key = `${cube.domain}.${cube.name}`;

    if (format === 'jinja') {
      const loop = (body: string[]) => ['{%- for i in range(COMPILE_CONTEXT.securityContext.copies) %}', ...body, '{%- endfor %}'];
      const load: Loader = (kind) => [
        `{%- for member in dynamic_members(${JSON.stringify(key)}, "${kind}", i) %}`,
        '      - {{ member | tojson }}',
        '{%- endfor %}',
      ];
      const lines = ['cubes:', ...loop(cubeToYaml(cube, members.static, suffixed('_{{ i }}'), load))];
      if (view) {
        lines.push('', 'views:', ...loop(viewToYaml(view, suffixed('_{{ i }}'))));
      }
      files.push({ fileName: `${cube.domain}/${cube.name}.yml`, content: `${lines.join('\n')}\n` });
    } else {
      for (let i = 0; i < copies; i++) {
        const name = suffixed(`_${i}`);
        const fileName = `${cube.domain}/${cube.name}_${i}`;
        if (format === 'yaml') {
          const lines = ['cubes:', ...cubeToYaml(cube, members.all, name)];
          if (view) {
            lines.push('', 'views:', ...viewToYaml(view, name));
          }
          files.push({ fileName: `${fileName}.yml`, content: `${lines.join('\n')}\n` });
        } else {
          const load = (kind: MemberKind) => `dynamicMembers(${JSON.stringify(key)}, '${kind}', '_${i}')`;
          const parts = [
            'import { dynamicMembers } from \'./dynamic_members\';',
            cubeToJs(cube, members.static, name, load),
            ...(view ? [viewToJs(view, name)] : []),
          ];
          files.push({ fileName: `${fileName}.js`, content: `${parts.join('\n\n')}\n` });
        }
      }
    }
  }

  return {
    files,
    cubes: cubes.length * copies,
    views: withViews ? views.size * copies : 0,
    cubeMembers: cubes.length * MEMBERS_PER_CUBE * copies,
    dynamicMembers: format === 'yaml' ? 0 : cubes.length * (MEMBERS_PER_CUBE - STATIC_MEMBERS_PER_CUBE) * copies,
  };
}
