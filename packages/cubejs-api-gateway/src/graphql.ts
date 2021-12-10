import R from 'ramda';
import moment from 'moment-timezone';

import {
  GraphQLResolveInfo,
  ArgumentNode,
  DirectiveNode,
  FieldNode,
  VariableNode,
  ValueNode,
} from 'graphql';

import {
  objectType,
  enumType,
  extendType,
  list,
  inputObjectType,
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
} from 'graphql-scalars';

import { QUERY_TYPE } from './query';

const DateTimeScalar = asNexusMethod(DateTimeResolver, 'date');

const FloatFilter = inputObjectType({
  name: 'FloatFilter',
  definition(t) {
    t.float('equals');
    t.float('notEquals');
    t.list.float('in');
    t.list.float('notIn');
    t.boolean('set');
    t.float('gt');
    t.float('lt');
    t.float('gte');
    t.float('lte');
  }
});

const StringFilter = inputObjectType({
  name: 'StringFilter',
  definition(t) {
    t.string('equals');
    t.string('notEquals');
    t.list.string('in');
    t.list.string('notIn');
    t.string('contains');
    t.string('notContains');
    t.boolean('set');
  }
});

const DateTimeFilter = inputObjectType({
  name: 'DateTimeFilter',
  definition(t) {
    t.string('equals');
    t.string('notEquals');
    t.list.string('in');
    t.list.string('notIn');
    t.list.string('inDateRange');
    t.list.string('notInDateRange');
    t.string('beforeDate');
    t.string('afterDate');
    t.boolean('set');
  }
});

const OrderBy = enumType({
  name: 'OrderBy',
  members: [
    'asc',
    'desc'
  ]
});

export const TimeDimension = objectType({
  name: 'TimeDimension',
  definition(t) {
    t.nonNull.field('value', {
      type: 'DateTime',
    });
    t.nonNull.field('second', {
      type: 'DateTime',
    });
    t.nonNull.field('minute', {
      type: 'DateTime',
    });
    t.nonNull.field('hour', {
      type: 'DateTime',
    });
    t.nonNull.field('day', {
      type: 'DateTime',
    });
    t.nonNull.field('week', {
      type: 'DateTime',
    });
    t.nonNull.field('month', {
      type: 'DateTime',
    });
    t.nonNull.field('quarter', {
      type: 'DateTime'
    });
    t.nonNull.field('year', {
      type: 'DateTime',
    });
  },
});

function mapType(type: string, isInputType?: boolean) {
  switch (type) {
    case 'time':
      return isInputType ? 'DateTime' : 'TimeDimension';
    case 'string':
      return 'String';
    case 'number':
      return 'Float';
    default:
      return 'String';
  }
}

function mapWhereOperator(operator: string, value: any) {
  switch (operator) {
    case 'in':
      return 'equals';
    case 'notIn':
      return 'notEquals';
    case 'set':
      return (value === true) ? 'set' : 'notSet';
    default:
      return operator;
  }
}

function mapWhereValue(operator: string, value: any) {
  switch (operator) {
    case 'set':
      return undefined;
    default:
      return Array.isArray(value) ? value.map(v => `${v}`) : [`${value}`];
  }
}

function safeName(name: string) {
  return name.split('.').slice(1).join('');
}

function capitalize(name: string) {
  return `${name[0].toUpperCase()}${name.slice(1)}`;
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
  return (node.selectionSet?.selections.filter((childNode) => (
    childNode.kind === 'Field' &&
    childNode.name.value !== '__typename' &&
    applyDirectives(childNode.directives, infos.variableValues)
  )) || []) as FieldNode[];
}

function parseArgumentValue(value: ValueNode) {
  switch (value.kind) {
    case 'BooleanValue':
    case 'IntValue':
    case 'StringValue':
    case 'FloatValue':
    case 'EnumValue':
      return value.value;
    case 'ListValue':
      return value.values.map(v => parseArgumentValue(v));
    case 'ObjectValue':
      return value.fields.reduce((obj, v) => ({
        ...obj,
        [v.name.value]: parseArgumentValue(v.value),
      }), {});
    default:
      return undefined;
  }
}

function getArgumentValue(node: FieldNode, argName: string) {
  const argument = node.arguments?.find(a => a.name.value === argName)?.value;
  return argument ? parseArgumentValue(argument) : argument;
}

function getMemberType(metaConfig: any, cubeName: string, memberName: string) {
  const cubeConfig = metaConfig.find(cube => cube.config.name === cubeName);
  if (!cubeConfig) return undefined;

  return ['measure', 'dimension'].find((memberType) => (cubeConfig.config[`${memberType}s`]
    .findIndex(entry => entry.name === `${cubeName}.${memberName}`) !== -1
  ));
}

function whereArgToQueryFilters(
  whereArg: Record<string, any>,
  prefix?: string
) {
  const queryFilters: any[] = [];

  Object.keys(whereArg).forEach((key) => {
    if (['OR', 'AND'].includes(key)) {
      queryFilters.push({
        [key.toLowerCase()]: whereArg[key].reduce(
          (filters, whereBooleanArg) => [
            ...filters,
            ...whereArgToQueryFilters(whereBooleanArg, prefix),
          ],
          []
        ),
      });
    } else if (whereArg[key].OR || whereArg[key].AND) {
      // users: {
      //   OR: {
      //     name: { equals: "Alex" }
      //     country: { equals: "US" }
      //   } # <-- a single boolean filter can be passed in directly
      //   age: { equals: 28 } # <-- will require AND
      // }
      if (Object.keys(whereArg[key]).length > 1) {
        queryFilters.push(
          ...whereArgToQueryFilters(
            {
              AND: Object.entries(whereArg[key]).reduce<any>(
                (memo, [k, v]) => [...memo, { [k]: v }],
                []
              ),
            },
            capitalize(key)
          )
        );
      } else {
        const res = whereArgToQueryFilters(whereArg[key], capitalize(key));

        queryFilters.push(...res);
      }
    } else if (prefix) {
      // handle a subfilter
      // { country: { in: ["US"] }
      Object.entries(whereArg[key]).forEach(([operator, value]) => {
        queryFilters.push({
          member: `${prefix}.${key}`,
          operator: mapWhereOperator(operator, value),
          ...(mapWhereValue(operator, value) && {
            values: mapWhereValue(operator, value),
          }),
        });
      });
    } else {
      Object.entries<any>(whereArg[key]).forEach(([member, filters]) => {
        Object.entries(filters).forEach(([operator, value]) => {
          queryFilters.push({
            member: prefix
              ? `${prefix}.${key}`
              : `${capitalize(key)}.${member}`,
            operator: mapWhereOperator(operator, value),
            ...(mapWhereValue(operator, value) && {
              values: mapWhereValue(operator, value),
            }),
          });
        });
      });
    }
  });

  return queryFilters;
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
    FloatFilter,
    StringFilter,
    DateTimeFilter,
    OrderBy,
    TimeDimension
  ];

  metaConfig.forEach(cube => {
    types.push(objectType({
      name: `${cube.config.name}Members`,
      definition(t) {
        cube.config.measures.forEach(measure => {
          if (measure.isVisible) {
            t.nonNull.field(safeName(measure.name), {
              type: mapType(measure.type),
              description: measure.description
            });
          }
        });
        cube.config.dimensions.forEach(dimension => {
          if (dimension.isVisible) {
            t.nonNull.field(safeName(dimension.name), {
              type: mapType(dimension.type),
              description: dimension.description
            });
          }
        });
      }
    }));

    types.push(inputObjectType({
      name: `${cube.config.name}WhereInput`,
      definition(t) {
        t.field('AND', { type: list(nonNull(`${cube.config.name}WhereInput`)) });
        t.field('OR', { type: list(nonNull(`${cube.config.name}WhereInput`)) });
        cube.config.measures.forEach(measure => {
          if (measure.isVisible) {
            t.field(safeName(measure.name), {
              type: `${mapType(measure.type, true)}Filter`,
            });
          }
        });
        cube.config.dimensions.forEach(dimension => {
          if (dimension.isVisible) {
            t.field(safeName(dimension.name), {
              type: `${mapType(dimension.type, true)}Filter`,
            });
          }
        });
      }
    }));

    types.push(inputObjectType({
      name: `${cube.config.name}OrderByInput`,
      definition(t) {
        cube.config.measures.forEach(measure => {
          if (measure.isVisible) {
            t.field(safeName(measure.name), {
              type: 'OrderBy',
            });
          }
        });
        cube.config.dimensions.forEach(dimension => {
          if (dimension.isVisible) {
            t.field(safeName(dimension.name), {
              type: 'OrderBy',
            });
          }
        });
      }
    }));
  });

  types.push(inputObjectType({
    name: 'RootWhereInput',
    definition(t) {
      t.field('AND', { type: list(nonNull('RootWhereInput')) });
      t.field('OR', { type: list(nonNull('RootWhereInput')) });
      metaConfig.forEach(cube => {
        t.field(unCapitalize(cube.config.name), {
          type: `${cube.config.name}WhereInput`
        });
      });
    }
  }));
  
  types.push(inputObjectType({
    name: 'RootOrderByInput',
    definition(t) {
      metaConfig.forEach(cube => {
        t.field(unCapitalize(cube.config.name), {
          type: `${cube.config.name}OrderByInput`
        });
      });
    }
  }));

  types.push(objectType({
    name: 'Result',
    definition(t) {
      metaConfig.forEach(cube => {
        t.nonNull.field(unCapitalize(cube.config.name), {
          type: `${cube.config.name}Members`,
          args: {
            where: arg({
              type: `${cube.config.name}WhereInput`
            }),
            orderBy: arg({
              type: `${cube.config.name}OrderByInput`
            }),
          }
        });
      });
    }
  }));

  types.push(extendType({
    type: 'Query',
    definition(t) {
      t.nonNull.field('cube', {
        type: list(nonNull('Result')),
        args: {
          where: arg({
            type: 'RootWhereInput'
          }),
          limit: intArg(),
          offset: intArg(),
          timezone: stringArg(),
          renewQuery: booleanArg(),
          orderBy: arg({
            type: 'RootOrderByInput'
          }),
        },
        resolve: async (_, { where, limit, offset, timezone, orderBy, renewQuery }, { req, apiGateway }, infos) => {
          const measures: string[] = [];
          const dimensions: string[] = [];
          const timeDimensions: any[] = [];
          let filters: any[] = [];
          const order: [string, 'asc' | 'desc'][] = [];
          
          if (where) {
            filters = whereArgToQueryFilters(where);
          }
          
          if (orderBy) {
            Object.entries<any>(orderBy).forEach(([cubeName, members]) => {
              Object.entries<any>(members).forEach(([member, value]) => {
                order.push([`${capitalize(cubeName)}.${member}`, value]);
              });
            });
          }

          getFieldNodeChildren(infos.fieldNodes[0], infos).forEach(cubeNode => {
            const cubeName = capitalize(cubeNode.name.value);
            const orderByArg = getArgumentValue(cubeNode, 'orderBy');
            // todo: throw if both RootOrderByInput and [Cube]OrderByInput provided
            if (orderByArg) {
              Object.keys(orderByArg).forEach(key => {
                order.push([`${cubeName}.${key}`, orderByArg[key]]);
              });
            }

            const whereArg = getArgumentValue(cubeNode, 'where');
            if (whereArg) {
              filters = whereArgToQueryFilters(whereArg, cubeName).concat(filters);
            }

            getFieldNodeChildren(cubeNode, infos).forEach(memberNode => {
              const memberName = memberNode.name.value;
              const memberType = getMemberType(metaConfig, cubeName, memberName);

              if (memberType === 'measure') {
                measures.push(`${cubeName}.${memberName}`);
              } else if (memberType === 'dimension') {
                const granularityNodes = getFieldNodeChildren(memberNode, infos);
                if (granularityNodes.length > 0) {
                  granularityNodes.forEach(granularityNode => {
                    const granularityName = granularityNode.name.value;
                    if (granularityName === 'value') {
                      dimensions.push(`${cubeName}.${memberName}`);
                    } else {
                      timeDimensions.push({
                        dimension: `${cubeName}.${memberName}`,
                        granularity: granularityName
                      });
                    }
                  });
                } else {
                  dimensions.push(`${cubeName}.${memberName}`);
                }
              }
            });
          });

          const query = {
            ...(measures.length && { measures }),
            ...(dimensions.length && { dimensions }),
            ...(timeDimensions.length && { timeDimensions }),
            ...(Object.keys(order).length && { order }),
            ...(limit && { limit }),
            ...(offset && { offset }),
            ...(timezone && { timezone }),
            ...(filters.length && { filters }),
            ...(renewQuery && { renewQuery }),
          };
          
          // eslint-disable-next-line no-async-promise-executor
          const results = await (new Promise<any>(async (resolve, reject) => {
            try {
              await apiGateway.load({
                query,
                queryType: QUERY_TYPE.REGULAR_QUERY,
                context: req.context,
                res: (message) => {
                  if (message.error) {
                    reject(new Error(message.error));
                  }
                  resolve(message);
                },
              });
            } catch (e) {
              reject(e);
            }
          }));

          parseDates(results);

          return results.data.map(entry => R.toPairs(entry)
            .reduce((res, pair) => {
              let path = pair[0].split('.');
              path[0] = unCapitalize(path[0]);
              if (results.annotation.dimensions[pair[0]]?.type === 'time') {
                path = [...path, 'value'];
              }
              return (results.annotation.timeDimensions[pair[0]] && path.length !== 3)
                ? res : R.set(R.lensPath(path), pair[1], res);
            }, {}));
        }
      });
    }
  }));

  return nexusMakeSchema({ types });
}
