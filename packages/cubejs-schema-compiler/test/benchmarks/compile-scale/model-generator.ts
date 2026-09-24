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
// Emitters

/** `name` of a cube or view within one copy; `suffix` is `_3` or `_{{ i }}` */
type Namer = (name: string) => string;

const resolveRefs = (sql: string, name: Namer) => sql.replace(/\{@(\w+)/g, (_, cube) => `{${name(cube)}`);

const yamlString = (s: string) => JSON.stringify(s);

function cubeToYaml(cube: CubeSpec, view: ViewSpec | undefined, name: Namer, indent: string): { cube: string; view?: string } {
  const lines: string[] = [];
  const out = (depth: number, line: string) => lines.push(`${indent}${'  '.repeat(depth)}${line}`);

  out(0, `- name: ${name(cube.name)}`);
  out(1, `sql_table: ${cube.sqlTable}`);
  if (cube.joins.length) {
    out(1, 'joins:');
    for (const join of cube.joins) {
      out(2, `- name: ${name(join.target)}`);
      out(3, 'relationship: many_to_one');
      out(3, `sql: ${yamlString(resolveRefs(join.sql, name))}`);
    }
  }
  out(1, 'dimensions:');
  for (const d of cube.dimensions) {
    out(2, `- name: ${d.name}`);
    out(3, `type: ${d.type}`);
    if (d.sql) out(3, `sql: ${yamlString(resolveRefs(d.sql, name))}`);
    if (d.primaryKey) out(3, 'primary_key: true');
    if (d.case) {
      out(3, 'case:');
      out(4, 'when:');
      for (const w of d.case.when) {
        out(5, `- sql: ${yamlString(w.sql)}`);
        out(6, `label: ${w.label}`);
      }
      out(4, 'else:');
      out(5, `label: ${d.case.else}`);
    }
    if (d.granularities) {
      out(3, 'granularities:');
      for (const g of d.granularities) {
        out(4, `- name: ${g.name}`);
        out(5, `interval: ${g.interval}`);
        if (g.offset) out(5, `offset: ${g.offset}`);
        if (g.origin) out(5, `origin: ${yamlString(g.origin)}`);
      }
    }
  }
  out(1, 'measures:');
  for (const m of cube.measures) {
    out(2, `- name: ${m.name}`);
    out(3, `type: ${m.type}`);
    if (m.sql) out(3, `sql: ${yamlString(m.sql)}`);
    if (m.format) out(3, `format: ${m.format}`);
    if (m.filters) {
      out(3, 'filters:');
      for (const f of m.filters) out(4, `- sql: ${yamlString(f)}`);
    }
    if (m.rollingWindow) {
      out(3, 'rolling_window:');
      out(4, `trailing: ${m.rollingWindow}`);
    }
  }
  out(1, 'segments:');
  for (const s of cube.segments) {
    out(2, `- name: ${s.name}`);
    out(3, `sql: ${yamlString(s.sql)}`);
  }
  if (cube.preAggregations.length) {
    out(1, 'pre_aggregations:');
    for (const p of cube.preAggregations) {
      out(2, `- name: ${p.name}`);
      out(3, `measures: [${p.measures.join(', ')}]`);
      out(3, `dimensions: [${p.dimensions.join(', ')}]`);
      out(3, `time_dimension: ${p.timeDimension}`);
      out(3, `granularity: ${p.granularity}`);
      out(3, `partition_granularity: ${p.partitionGranularity}`);
    }
  }

  if (!view) {
    return { cube: lines.join('\n') };
  }

  const viewLines: string[] = [];
  const vout = (depth: number, line: string) => viewLines.push(`${indent}${'  '.repeat(depth)}${line}`);
  vout(0, `- name: ${name(view.name)}`);
  vout(1, 'cubes:');
  for (const jp of view.joinPaths) {
    vout(2, `- join_path: ${jp.path.map(name).join('.')}`);
    vout(3, jp.includes === '*' ? 'includes: "*"' : `includes: [${jp.includes.join(', ')}]`);
    if (jp.prefix) vout(3, 'prefix: true');
  }

  return { cube: lines.join('\n'), view: viewLines.join('\n') };
}

const jsString = (s: string) => `\`${s.replace(/`/g, '\\`')}\``;

/** `{CUBE}.x`, `{member}`, `{cube.member}` to JS template interpolations */
const toJsSql = (sql: string, name: Namer) => jsString(resolveRefs(sql, name).replace(/\{([\w.]+)\}/g, (_, ref) => `\${${ref}}`));

function cubeToJs(cube: CubeSpec, view: ViewSpec | undefined, name: Namer): string {
  const lines: string[] = [];
  const out = (depth: number, line: string) => lines.push(`${'  '.repeat(depth)}${line}`);

  out(0, `cube(\`${name(cube.name)}\`, {`);
  out(1, `sql_table: \`${cube.sqlTable}\`,`);
  if (cube.joins.length) {
    out(1, 'joins: {');
    for (const join of cube.joins) {
      out(2, `${name(join.target)}: { relationship: \`many_to_one\`, sql: ${toJsSql(join.sql, name)} },`);
    }
    out(1, '},');
  }
  out(1, 'dimensions: {');
  for (const d of cube.dimensions) {
    const props = [`type: \`${d.type}\``];
    if (d.sql) props.push(`sql: ${toJsSql(d.sql, name)}`);
    if (d.primaryKey) props.push('primary_key: true');
    if (d.case) {
      props.push(`case: { when: [${d.case.when.map((w) => `{ sql: ${toJsSql(w.sql, name)}, label: \`${w.label}\` }`).join(', ')}], else: { label: \`${d.case.else}\` } }`);
    }
    if (d.granularities) {
      props.push(`granularities: { ${d.granularities.map((g) => `${g.name}: { interval: \`${g.interval}\`${g.offset ? `, offset: \`${g.offset}\`` : ''}${g.origin ? `, origin: \`${g.origin}\`` : ''} }`).join(', ')} }`);
    }
    out(2, `${d.name}: { ${props.join(', ')} },`);
  }
  out(1, '},');
  out(1, 'measures: {');
  for (const m of cube.measures) {
    const props = [`type: \`${m.type}\``];
    if (m.sql) props.push(`sql: ${toJsSql(m.sql, name)}`);
    if (m.format) props.push(`format: \`${m.format}\``);
    if (m.filters) props.push(`filters: [${m.filters.map((f) => `{ sql: ${toJsSql(f, name)} }`).join(', ')}]`);
    if (m.rollingWindow) props.push(`rolling_window: { trailing: \`${m.rollingWindow}\` }`);
    out(2, `${m.name}: { ${props.join(', ')} },`);
  }
  out(1, '},');
  out(1, 'segments: {');
  for (const s of cube.segments) {
    out(2, `${s.name}: { sql: ${toJsSql(s.sql, name)} },`);
  }
  out(1, '},');
  if (cube.preAggregations.length) {
    out(1, 'pre_aggregations: {');
    for (const p of cube.preAggregations) {
      out(2, `${p.name}: {`);
      out(3, `measures: [${p.measures.map((m) => `CUBE.${m}`).join(', ')}],`);
      out(3, `dimensions: [${p.dimensions.map((d) => `CUBE.${d}`).join(', ')}],`);
      out(3, `time_dimension: CUBE.${p.timeDimension},`);
      out(3, `granularity: \`${p.granularity}\`,`);
      out(3, `partition_granularity: \`${p.partitionGranularity}\`,`);
      out(2, '},');
    }
    out(1, '},');
  }
  out(0, '});');

  if (view) {
    out(0, '');
    out(0, `view(\`${name(view.name)}\`, {`);
    out(1, 'cubes: [');
    for (const jp of view.joinPaths) {
      const includes = jp.includes === '*' ? '`*`' : `[${jp.includes.map((i) => `\`${i}\``).join(', ')}]`;
      out(2, `{ join_path: ${jp.path.map(name).join('.')}, includes: ${includes}${jp.prefix ? ', prefix: true' : ''} },`);
    }
    out(1, '],');
    out(0, '});');
  }

  return lines.join('\n');
}

export type ModelFile = { fileName: string; content: string };

export type GeneratedModel = {
  files: ModelFile[];
  compileContext: Record<string, unknown>;
  cubes: number;
  views: number;
  cubeMembers: number;
};

/**
 * `jinja` writes 100 files, each looping `COMPILE_CONTEXT.copies` times over its cube: the
 * dynamic model. `yaml` and `js` write the same cubes pre-expanded, one file per cube, which is
 * how a static model of that size is usually laid out.
 */
export function generateModel(copies: number, format: ModelFormat, { views: withViews = true } = {}): GeneratedModel {
  const { cubes, views } = buildBaseModel();
  const files: ModelFile[] = [];
  const suffixed = (suffix: string): Namer => (n) => `${n}${suffix}`;

  for (const cube of cubes) {
    const view = withViews ? views.get(cube.name) : undefined;

    if (format === 'jinja') {
      const { cube: cubeYaml, view: viewYaml } = cubeToYaml(cube, view, suffixed('_{{ i }}'), '  ');
      const loop = (body: string) => ['{%- for i in range(COMPILE_CONTEXT.copies) %}', body, '{%- endfor %}'].join('\n');
      const content = ['cubes:', loop(cubeYaml), ...(viewYaml ? ['', 'views:', loop(viewYaml)] : [])].join('\n');
      files.push({ fileName: `${cube.domain}/${cube.name}.yml`, content: `${content}\n` });
    } else {
      for (let i = 0; i < copies; i++) {
        const fileName = `${cube.domain}/${cube.name}_${i}`;
        if (format === 'yaml') {
          const part = cubeToYaml(cube, view, suffixed(`_${i}`), '  ');
          const content = ['cubes:', part.cube, ...(part.view ? ['', 'views:', part.view] : [])].join('\n');
          files.push({ fileName: `${fileName}.yml`, content: `${content}\n` });
        } else {
          files.push({ fileName: `${fileName}.js`, content: `${cubeToJs(cube, view, suffixed(`_${i}`))}\n` });
        }
      }
    }
  }

  return {
    files,
    compileContext: { copies },
    cubes: cubes.length * copies,
    views: withViews ? views.size * copies : 0,
    cubeMembers: cubes.length * MEMBERS_PER_CUBE * copies,
  };
}
