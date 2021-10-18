import R from 'ramda';
import moment from 'moment-timezone';
import fetch from 'node-fetch';
import { encode } from 'querystring';

import {
  GraphQLResolveInfo,
  ArgumentNode,
  DirectiveNode,
  FieldNode,
  VariableNode,
  StringValueNode
} from 'graphql';

import {
  objectType,
  enumType,
  extendType,
  list,
  inputObjectType,
  nullable,
  nonNull,
  arg,
  intArg,
  stringArg,
  booleanArg,
  makeSchema as nexusMakeSchema,
  asNexusMethod
} from 'nexus';

import {
  DateTimeResolver,
  SafeIntResolver
} from 'graphql-scalars';

const DateTimeScalar = asNexusMethod(DateTimeResolver, 'date');
const SafeIntScalar = asNexusMethod(SafeIntResolver, 'safeInt');

const CubeFilterInput = inputObjectType({
  name: 'CubeFilterInput',
  definition(t) {
    t.nonNull.string('member');
    t.nonNull.field('operator', { type: 'CubeFilterOperator' });
    t.field('values', { type: list(nullable('String')) });
    t.field('or', { type: list(nonNull('CubeFilterInput')) });
    t.field('and', { type: list(nonNull('CubeFilterInput')) });
  }
});

const CubeFilterOperator = enumType({
  name: 'CubeFilterOperator',
  members: [
    'equals',
    'notEquals',
    'contains',
    'notContains',
    'gt',
    'gte',
    'lt',
    'lte',
    'set',
    'notSet',
    'inDateRange',
    'notInDateRange',
    'beforeDate',
    'afterDate'
  ]
});

const CubeOrder = enumType({
  name: 'CubeOrder',
  members: [
    'asc',
    'desc'
  ]
});

const CubeGranularity = enumType({
  name: 'CubeGranularity',
  members: [
    'second',
    'minute',
    'day',
    'week',
    'month',
    'year'
  ]
});

function mapType(type: string) {
  switch (type) {
    case 'time':
      return 'DateTime';
    case 'string':
      return 'String';
    case 'number':
      return 'SafeInt';
    default:
      return 'String';
  }
}

function safeName(name: string) {
  return name.split('.').slice(1).join('');
}

function unCapitalize(name: string) {
  return `${name[0].toLowerCase()}${name.slice(1)}`;
}

function applyDirectives(
  directives: readonly DirectiveNode[] | undefined,
  values: Record<string, any>
) {
  if (directives === undefined || directives.length === 0) {
    return true;
  }

  return directives.reduce((result, directive) => {
    directive.arguments?.forEach((argument: ArgumentNode) => {
      if (argument.name.value === 'if') {
        if (
          (directive.name.value === 'include' &&
            !values[(argument.value as VariableNode).name.value]) ||
          (directive.name.value === 'skip' &&
            values[(argument.value as VariableNode).name.value])
        ) {
          result = false;
        }
      }
    });
    return result;
  }, true);
}

function getFieldNodeChildren(node: FieldNode, infos: GraphQLResolveInfo) {
  return node.selectionSet?.selections.filter((childNode) => (
    childNode.kind === 'Field' &&
    childNode.name.value !== '__typename' &&
    applyDirectives(childNode.directives, infos.variableValues)
  )) as FieldNode[];
}

function getFieldNodeByName(name: string, infos: GraphQLResolveInfo) {
  return infos.fieldNodes[0].selectionSet?.selections.find(
    (node) => node.kind === 'Field' &&
      (node as FieldNode).name.value === name &&
      applyDirectives(node.directives, infos.variableValues)
  ) as FieldNode | undefined;
}

function getMeasures(infos: GraphQLResolveInfo) {
  const fieldNode = getFieldNodeByName('measures', infos);
  if (!fieldNode) return [];

  return getFieldNodeChildren(fieldNode, infos)
    .map(node => node.name.value) || [];
}

function getDimensions(infos: GraphQLResolveInfo) {
  const fieldNode = getFieldNodeByName('dimensions', infos);
  if (!fieldNode) return [];

  return getFieldNodeChildren(fieldNode, infos)
    .map(node => node.name.value) || [];
}

function getChildrenFieldsWithArg(name: string, argument: string, infos: GraphQLResolveInfo) {
  const fieldNode = getFieldNodeByName(name, infos);
  if (!fieldNode) return [];

  return getFieldNodeChildren(fieldNode, infos)
    .reduce((res, node) => {
      const foundArg = node.arguments?.find(a => a.name.value === argument);
      if (foundArg) {
        return [...res, [node.name.value, (foundArg.value as StringValueNode).value]];
      }
      return res;
    }, [] as string[][]);
}

function getFieldsWithArg(argument: string, infos: GraphQLResolveInfo) {
  return [
    ...getChildrenFieldsWithArg('measures', argument, infos),
    ...getChildrenFieldsWithArg('dimensions', argument, infos),
  ];
}

export async function proxifyQuery(
  query: any,
  endpoint: string,
  originalReq: any,
  delay = 500
): Promise<any> {
  const url = `${endpoint}?${encode({
    query: JSON.stringify(query)
  })}`;

  const headers = { ...originalReq.headers } as Record<string, string>;
  ['host', 'connexion', 'content-length'].forEach(key => delete headers[key]);

  const response = await fetch(url, {
    method: 'GET',
    headers
  });

  if (!response.ok) {
    if (response.headers.get('content-type')?.includes('application/json')) {
      const json = await response.json();
      if (json.error) {
        throw Error(`Error querying load api: ${json.error}`);
      }
    }
    throw Error(`Error querying load api: status ${response.status}`);
  }

  const json = await response.json();

  if (json.error === 'Continue wait') {
    await new Promise((resolve) => setTimeout(resolve, delay));
    return proxifyQuery(query, endpoint, originalReq, delay * 1.2);
  }

  return json;
}

function parseDates(result: any) {
  const { timezone } = result.query;

  const dateKeys = Object.entries<any>({
    ...result.annotation.measures,
    ...result.annotation.dimensions,
    ...result.annotation.timeDimensions,
  }).reduce((res, [key, value]) => (value.type === 'time' ? [...res, key] : res), [] as any);

  result.data.forEach(row => {
    Object.keys(row).forEach(key => {
      if (dateKeys.includes(key)) {
        row[key] = moment.tz(row[key], timezone).toISOString();
      }
      return row;
    });
  });
}

export function makeSchema(metaConfig: any) {
  const types: any[] = [
    DateTimeScalar,
    SafeIntScalar,
    CubeFilterInput,
    CubeFilterOperator,
    CubeOrder,
    CubeGranularity
  ];

  metaConfig.forEach(cube => {
    types.push(objectType({
      name: `${cube.config.name}Measures`,
      description: `${cube.config.title} measures`,
      definition(t) {
        cube.config.measures.forEach(measure => {
          if (measure.isVisible) {
            t.field(safeName(measure.name), {
              type: mapType(measure.type),
              args: { order: arg({ type: 'CubeOrder' }) }
            });
          }
        });
      }
    }));

    types.push(objectType({
      name: `${cube.config.name}Dimensions`,
      description: `${cube.config.title} dimensions`,
      definition(t) {
        cube.config.dimensions.forEach(dimension => {
          if (dimension.isVisible) {
            t.field(safeName(dimension.name), {
              type: mapType(dimension.type),
              args: {
                order: arg({ type: 'CubeOrder' }),
                ...(dimension.type === 'time' && {
                  granularity: arg({ type: 'CubeGranularity' })
                })
              }
            });
          }
        });
      }
    }));

    types.push(objectType({
      name: cube.config.name,
      description: cube.config.description,
      definition(t) {
        t.nonNull.field('measures', {
          type: `${cube.config.name}Measures`,
          resolve: (source) => source
        });
        t.nonNull.field('dimensions', {
          type: `${cube.config.name}Dimensions`,
          resolve: (source) => source
        });
      }
    }));

    types.push(extendType({
      type: 'Query',
      definition(t) {
        t.nonNull.field(unCapitalize(cube.config.name), {
          type: list(nonNull(cube.config.name)),
          args: {
            filters: arg({
              type: list(nonNull('CubeFilterInput'))
            }),
            limit: intArg(),
            offset: intArg(),
            timezone: stringArg(),
            renewQuery: booleanArg(),
          },
          resolve: async (parent, { filters, limit, offset, timezone, renewQuery }, context, infos) => {
            const measures = getMeasures(infos);
            const dimensions = getDimensions(infos);
            const timeDimensions = getFieldsWithArg('granularity', infos);
            const orders = getFieldsWithArg('orders', infos);

            if (timeDimensions.length > 1) {
              throw new Error('You must set only one dimension with granularity');
            }

            const query = {
              ...(measures.length && {
                measures: measures.map(measure => `${cube.config.name}.${measure}`)
              }),
              ...(dimensions.length && {
                dimensions: dimensions
                  .filter(dimension => timeDimensions[0][0] !== dimension)
                  .map(dimension => `${cube.config.name}.${dimension}`)
              }),
              ...(timeDimensions.length && {
                timeDimensions: timeDimensions.map(timeDimension => ({
                  dimension: `${cube.config.name}.${timeDimension[0]}`,
                  granularity: timeDimension[1]
                })),
              }),
              ...(filters && {
                filters: filters.map(filter => ({ ...filter, member: `${cube.config.name}.${filter.member}` }))
              }),
              ...(limit && { limit }),
              ...(offset && { offset }),
              ...(orders.length && {
                order: R.fromPairs(orders.map(order => [`${cube.config.name}.${order[0]}`, order[1]]))
              }),
              ...(timezone && { timezone }),
              ...(renewQuery && { renewQuery }),
            };

            const results = await proxifyQuery(query, context.endpoint, context.req);
            parseDates(results);

            return results.data.map(entry => R.fromPairs(R.toPairs(entry).map(pair => [pair[0].split('.').slice(1).join('.'), pair[1]])));
          }
        });
      }
    }));
  });

  return nexusMakeSchema({ types });
}
