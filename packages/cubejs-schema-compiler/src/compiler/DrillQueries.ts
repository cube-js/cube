import { UserError } from './UserError';

/**
 * Named drill queries on measures (`drill_queries`) and the member sets they
 * use (`member_sets`).
 *
 * A drill query names members as dotted paths. In a cube, a base name is a
 * member or member set of the cube and a qualified name is a join path from
 * the cube (`users.city`). In a view, a base name is a member or member set of
 * the view and a qualified name is a member or member set of another view or
 * cube (`customer_details.contact`). Member sets expand at compile time.
 */

export type DrillQueryOrderDirection = 'asc' | 'desc';

export type DrillQueryFilter =
  { member: string; operator: string; values?: unknown[] } |
  { and: DrillQueryFilter[] } |
  { or: DrillQueryFilter[] };

export type DrillQueryConfig = {
  name: string;
  title: string;
  description?: string;
  icon?: string;
  members: string[];
  membersGrouped: {
    dimensions: string[];
    measures: string[];
  };
  order?: Record<string, DrillQueryOrderDirection>;
  limit?: number;
  filters?: DrillQueryFilter[];
};

/**
 * The model as drill queries see it: works on both the cube symbols built
 * before views include their members and the fully evaluated cubes.
 */
export interface DrillQueryModel {
  /** `true` for a view, `false` for a cube, `undefined` when neither exists */
  isView(cubeName: string): boolean | undefined;
  member(cubeName: string, memberName: string): { kind: 'dimensions' | 'measures' | 'segments'; type?: string } | undefined;
  memberSets(cubeName: string): Array<{ name: string; members: unknown }>;
  /** The cube a declared join (by its name, or its alias when it has one) leads to */
  joinTarget(cubeName: string, joinName: string): string | undefined;
  funcArguments(fn: Function): string[];
}

/** A member a name resolved to */
export type DrillTarget = {
  /** What identifies the member in its scope: the join path for a cube, the qualified name for a view */
  key: string;
  /** The qualified name: `cube.member` or `view.member` */
  name: string;
  kind: 'dimensions' | 'measures';
  type?: string;
};

type ResolvedFilter =
  { member: DrillTarget; operator: string; values?: unknown[] } |
  { and: ResolvedFilter[] } |
  { or: ResolvedFilter[] };

export type ResolvedDrillQuery = {
  name: string;
  title?: string;
  description?: string;
  icon?: string;
  limit?: number;
  members: DrillTarget[];
  order: Array<[DrillTarget, DrillQueryOrderDirection]>;
  filters: ResolvedFilter[];
};

const REFERENCE_PATH = Symbol('cube.drillQueryReferencePath');

// A JS model writes members as references (`members: [id, users.city]`): the
// transpiler wraps them into a function whose arguments are every identifier
// used. Each argument becomes a reference that collects the path read off it.
function reference(path: string[]): unknown {
  return new Proxy(Object.create(null), {
    get(_target, prop) {
      if (prop === REFERENCE_PATH) {
        return path;
      }
      if (prop === Symbol.toPrimitive || prop === 'toString') {
        return () => path.join('.');
      }
      if (typeof prop === 'symbol') {
        return undefined;
      }
      return reference([...path, prop]);
    },
  });
}

function nameOf(value: unknown): string | undefined {
  if (typeof value === 'string') {
    return value;
  }
  if (value && typeof value === 'object') {
    const path = (value as Record<symbol, unknown>)[REFERENCE_PATH];
    if (Array.isArray(path)) {
      return path.join('.');
    }
  }
  return undefined;
}

function evaluate(value: unknown, model: DrillQueryModel): unknown {
  if (typeof value === 'function') {
    return value(...model.funcArguments(value).map(arg => reference([arg])));
  }
  return value;
}

function names(value: unknown, model: DrillQueryModel, what: string): string[] {
  const evaluated = evaluate(value, model);
  if (!Array.isArray(evaluated)) {
    throw new UserError(`${what} must be a list of member names`);
  }
  return evaluated.map(item => {
    const name = nameOf(item);
    if (!name) {
      throw new UserError(`${what} must be a list of member names, found ${JSON.stringify(item)}`);
    }
    return name;
  });
}

type RawFilter = { member: string; operator: string; values?: unknown[] } | { and: RawFilter[] } | { or: RawFilter[] };

function rawFilters(value: unknown, what: string): RawFilter[] {
  if (!Array.isArray(value)) {
    throw new UserError(`${what} must be a list of filters`);
  }
  return value.map((filter): RawFilter => {
    if (!filter || typeof filter !== 'object') {
      throw new UserError(`${what} must be a list of filters, found ${JSON.stringify(filter)}`);
    }
    if (Array.isArray(filter.and)) {
      return { and: rawFilters(filter.and, what) };
    }
    if (Array.isArray(filter.or)) {
      return { or: rawFilters(filter.or, what) };
    }
    const member = nameOf(filter.member);
    if (!member || typeof filter.operator !== 'string') {
      throw new UserError(`${what}: each filter needs a member and an operator`);
    }
    return {
      member,
      operator: filter.operator,
      ...(filter.values !== undefined ? { values: filter.values } : {}),
    };
  });
}

function rawOrder(value: unknown, what: string): Array<[string, string]> {
  if (value === undefined || value === null) {
    return [];
  }
  const entries: unknown[][] = Array.isArray(value) ? value : Object.entries(value);
  return entries.map(entry => {
    const name = Array.isArray(entry) ? nameOf(entry[0]) : undefined;
    if (!name || !Array.isArray(entry) || (entry[1] !== 'asc' && entry[1] !== 'desc')) {
      throw new UserError(`${what} must map members to asc or desc`);
    }
    return [name, entry[1]];
  });
}

const DATE_OPERATORS = new Set(['inDateRange', 'notInDateRange', 'beforeDate', 'beforeOrOnDate', 'afterDate', 'afterOrOnDate']);
const STRING_OPERATORS = new Set(['contains', 'notContains', 'startsWith', 'notStartsWith', 'endsWith', 'notEndsWith']);
const COMPARISON_OPERATORS = new Set(['gt', 'gte', 'lt', 'lte']);
const ANY_TYPE_OPERATORS = new Set(['equals', 'notEquals', 'set', 'notSet']);

function memberDataType(target: DrillTarget): string | undefined {
  if (target.kind === 'dimensions') {
    return target.type;
  }
  // The data type of a measure, as `CubeSymbols.toMemberDataType` gives it
  return ['string', 'time', 'boolean'].includes(target.type || '') ? target.type : 'number';
}

function checkOperator(target: DrillTarget, operator: string, values: unknown, what: string) {
  const type = memberDataType(target);
  const fits = ANY_TYPE_OPERATORS.has(operator) ||
    (DATE_OPERATORS.has(operator) && type === 'time') ||
    (STRING_OPERATORS.has(operator) && type === 'string') ||
    (COMPARISON_OPERATORS.has(operator) && (type === 'number' || type === 'time'));
  if (!fits) {
    const known = ANY_TYPE_OPERATORS.has(operator) || DATE_OPERATORS.has(operator) ||
      STRING_OPERATORS.has(operator) || COMPARISON_OPERATORS.has(operator);
    throw new UserError(known
      ? `${what}: operator '${operator}' doesn't fit '${target.name}' of type ${type}`
      : `${what}: unknown filter operator '${operator}'`);
  }
  if (operator !== 'set' && operator !== 'notSet' && !(Array.isArray(values) && values.length)) {
    throw new UserError(`${what}: operator '${operator}' on '${target.name}' needs values`);
  }
}

export type DrillNameResolver = (name: string, allowSets: boolean) => DrillTarget[];

function expandSet(
  model: DrillQueryModel,
  cubeName: string,
  setName: string,
  stack: string[],
  resolveMember: (name: string, stack: string[]) => DrillTarget[]
): DrillTarget[] | undefined {
  const set = model.memberSets(cubeName).find(s => s.name === setName);
  if (!set) {
    return undefined;
  }
  const id = `${cubeName}.${setName}`;
  if (stack.includes(id)) {
    throw new UserError(`Member set '${id}' includes itself: ${[...stack, id].join(' -> ')}`);
  }
  return names(set.members, model, `Member set '${id}' members`)
    .flatMap(name => resolveMember(name, [...stack, id]));
}

/**
 * Resolves a name in a cube drill query or a cube member set: a member or
 * member set of the cube, or a join path from the cube to one.
 */
export function resolveCubeName(
  model: DrillQueryModel,
  cubeName: string,
  name: string,
  allowSets = true,
  stack: string[] = []
): DrillTarget[] {
  let parts = name.split('.');
  if (parts.length > 1 && (parts[0] === 'CUBE' || parts[0] === cubeName)) {
    parts = parts.slice(1);
  }

  let current = cubeName;

  for (const segment of parts.slice(0, -1)) {
    const target = model.joinTarget(current, segment);
    if (!target) {
      if (model.isView(segment)) {
        throw new UserError(`'${name}' names view '${segment}': a cube drill query can only name members of its cube and of the cubes it joins`);
      }
      throw new UserError(`'${name}' can't be reached by joins: cube '${current}' has no join '${segment}'`);
    }
    current = target;
  }

  const joinPath = parts.slice(0, -1);
  const leaf = parts[parts.length - 1];
  const member = model.member(current, leaf);

  if (member) {
    if (member.kind === 'segments') {
      throw new UserError(`'${name}' is a segment: drill queries name dimensions and measures`);
    }
    return [{
      key: [...joinPath, leaf].join('.'),
      name: `${current}.${leaf}`,
      kind: member.kind,
      type: member.type,
    }];
  }

  if (allowSets) {
    const expanded = expandSet(
      model,
      current,
      leaf,
      stack,
      (setMember, setStack) => resolveCubeName(model, current, setMember, true, setStack)
    );
    if (expanded) {
      return expanded.map(t => ({ ...t, key: [...joinPath, t.key].join('.') }));
    }
  }

  throw new UserError(`'${name}' is not a ${allowSets ? 'member or member set' : 'member'} of cube '${current}'`);
}

/**
 * Resolves a name in a view drill query or a view member set: a member or
 * member set of the view, or `<view or cube>.<member or member set>`.
 */
export function resolveViewName(
  model: DrillQueryModel,
  viewName: string,
  name: string,
  allowSets = true,
  stack: string[] = []
): DrillTarget[] {
  let parts = name.split('.');
  if (parts.length > 1 && parts[0] === 'CUBE') {
    parts = parts.slice(1);
  }
  if (parts.length > 2) {
    throw new UserError(`'${name}' must be a member of this view or '<view or cube>.<member>'`);
  }

  const owner = parts.length === 2 ? parts[0] : viewName;
  const leaf = parts[parts.length - 1];
  const ownerIsView = model.isView(owner);

  if (ownerIsView === undefined) {
    throw new UserError(`'${name}' names '${owner}', which is neither a view nor a cube`);
  }

  if (!ownerIsView) {
    return resolveCubeName(model, owner, leaf, allowSets, stack).map(t => ({ ...t, key: t.name }));
  }

  const member = model.member(owner, leaf);
  if (member) {
    if (member.kind === 'segments') {
      throw new UserError(`'${name}' is a segment: drill queries name dimensions and measures`);
    }
    const qualified = `${owner}.${leaf}`;
    return [{ key: qualified, name: qualified, kind: member.kind, type: member.type }];
  }

  if (allowSets) {
    const expanded = expandSet(
      model,
      owner,
      leaf,
      stack,
      (setMember, setStack) => resolveViewName(model, owner, setMember, true, setStack)
    );
    if (expanded) {
      return expanded;
    }
  }

  throw new UserError(`'${name}' is not a ${allowSets ? 'member or member set' : 'member'} of view '${owner}'`);
}

function uniqueByKey(targets: DrillTarget[]): DrillTarget[] {
  const seen = new Set<string>();
  return targets.filter(t => {
    if (seen.has(t.key)) {
      return false;
    }
    seen.add(t.key);
    return true;
  });
}

/**
 * Evaluates a drill query as written in the model and resolves its names.
 * Throws a `UserError` naming the first problem.
 */
export function resolveDrillQuery(
  model: DrillQueryModel,
  definition: any,
  resolve: DrillNameResolver
): ResolvedDrillQuery {
  const what = `Drill query '${definition?.name}'`;

  const members = uniqueByKey(names(definition.members, model, `${what} members`).flatMap(name => resolve(name, true)));
  if (!members.length) {
    throw new UserError(`${what} has no members`);
  }

  const memberKeys = new Set(members.map(m => m.key));
  const single = (name: string) => {
    const [target] = resolve(name, false);
    return target;
  };

  const order = rawOrder(evaluate(definition.order, model), `${what} order`)
    .map(([name, direction]): [DrillTarget, DrillQueryOrderDirection] => {
      const target = single(name);
      if (!memberKeys.has(target.key)) {
        throw new UserError(`${what} orders by '${name}', which is not in its members`);
      }
      return [target, direction as DrillQueryOrderDirection];
    });

  const resolveFilters = (filters: RawFilter[]): ResolvedFilter[] => filters.map((filter): ResolvedFilter => {
    if ('and' in filter) {
      return { and: resolveFilters(filter.and) };
    }
    if ('or' in filter) {
      return { or: resolveFilters(filter.or) };
    }
    const target = single(filter.member);
    checkOperator(target, filter.operator, filter.values, `${what} filter`);
    return { ...filter, member: target };
  });

  const filtersValue = evaluate(definition.filters, model);
  const filters = filtersValue === undefined || filtersValue === null
    ? []
    : resolveFilters(rawFilters(filtersValue, `${what} filters`));

  return {
    name: definition.name,
    ...(definition.title !== undefined ? { title: definition.title } : {}),
    ...(definition.description !== undefined ? { description: definition.description } : {}),
    ...(definition.icon !== undefined ? { icon: definition.icon } : {}),
    ...(definition.limit !== undefined ? { limit: definition.limit } : {}),
    members,
    order,
    filters,
  };
}

/** Every member a drill query names, its filters' members included */
export function drillQueryTargets(query: ResolvedDrillQuery): DrillTarget[] {
  const fromFilters = (filters: ResolvedFilter[]): DrillTarget[] => filters.flatMap(f => {
    if ('and' in f) {
      return fromFilters(f.and);
    }
    if ('or' in f) {
      return fromFilters(f.or);
    }
    return [f.member];
  });
  return [...query.members, ...query.order.map(([t]) => t), ...fromFilters(query.filters)];
}

/**
 * Renames the members of a drill query with `targetName`. A member it can't
 * name is dropped from `members` and `order`. The drill query is dropped
 * (`undefined`) when that would widen or reorder its rows: a filter member is
 * missing, an `order` member is missing while it has a `limit`, or no member
 * remains.
 */
export function mapDrillQuery(
  query: ResolvedDrillQuery,
  targetName: (target: DrillTarget) => string | undefined
): ResolvedDrillQuery | undefined {
  const rename = (target: DrillTarget): DrillTarget | undefined => {
    const name = targetName(target);
    return name === undefined ? undefined : { ...target, key: name, name };
  };

  const members = uniqueByKey(query.members.map(rename).filter((t): t is DrillTarget => !!t));
  if (!members.length) {
    return undefined;
  }

  const order: Array<[DrillTarget, DrillQueryOrderDirection]> = [];

  for (const [target, direction] of query.order) {
    const renamed = rename(target);
    if (renamed) {
      order.push([renamed, direction]);
    } else if (query.limit !== undefined) {
      return undefined;
    }
  }

  let missing = false;
  const mapFilters = (filters: ResolvedFilter[]): ResolvedFilter[] => filters.map((f): ResolvedFilter => {
    if ('and' in f) {
      return { and: mapFilters(f.and) };
    }
    if ('or' in f) {
      return { or: mapFilters(f.or) };
    }
    const renamed = rename(f.member);
    if (!renamed) {
      missing = true;
      return f;
    }
    return { ...f, member: renamed };
  });
  const filters = mapFilters(query.filters);

  return missing ? undefined : { ...query, members, order, filters };
}

/** The drill query as meta lists it */
export function drillQueryConfig(query: ResolvedDrillQuery, titleize: (name: string) => string): DrillQueryConfig {
  const membersGrouped = { dimensions: [] as string[], measures: [] as string[] };

  for (const member of query.members) {
    membersGrouped[member.kind].push(member.name);
  }

  const toConfigFilters = (filters: ResolvedFilter[]): DrillQueryFilter[] => filters.map((f): DrillQueryFilter => {
    if ('and' in f) {
      return { and: toConfigFilters(f.and) };
    }
    if ('or' in f) {
      return { or: toConfigFilters(f.or) };
    }
    return { ...f, member: f.member.name };
  });

  return {
    name: query.name,
    title: query.title ?? titleize(query.name),
    ...(query.description !== undefined ? { description: query.description } : {}),
    ...(query.icon !== undefined ? { icon: query.icon } : {}),
    members: query.members.map(m => m.name),
    membersGrouped,
    ...(query.order.length ? { order: Object.fromEntries(query.order.map(([t, d]) => [t.name, d])) } : {}),
    ...(query.limit !== undefined ? { limit: query.limit } : {}),
    ...(query.filters.length ? { filters: toConfigFilters(query.filters) } : {}),
  };
}
