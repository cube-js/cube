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
    t.float('contains');
    t.float('notContains');
    t.boolean('set');
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

const Granularity = enumType({
  name: 'Granularity',
  members: [
    'second',
    'minute',
    'hour',
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
  return node.selectionSet?.selections.filter((childNode) => (
    childNode.kind === 'Field' &&
    childNode.name.value !== '__typename' &&
    applyDirectives(childNode.directives, infos.variableValues)
  )) as FieldNode[];
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

function whereArgToQueryFilters(whereArg: any, prefix?: string) {
  const queryFilters: any[] = [];
  Object.keys(whereArg).forEach(member => {
    if (member === 'OR') {
      queryFilters.push({
        or: whereArg[member].reduce((filters, whereBooleanArg) => (
          [...filters, ...whereArgToQueryFilters(whereBooleanArg, prefix)]
        ), [] as any[])
      });
    } else if (member === 'AND') {
      queryFilters.push({
        and: whereArg[member].reduce((filters, whereBooleanArg) => (
          [...filters, ...whereArgToQueryFilters(whereBooleanArg, prefix)]
        ), [] as any[])
      });
    } else {
      Object.keys(whereArg[member]).forEach(operator => {
        const value = whereArg[member][operator];
        queryFilters.push({
          member: prefix ? `${prefix}.${member}` : member,
          operator: mapWhereOperator(operator, value),
          ...(mapWhereValue(operator, value) && {
            values: mapWhereValue(operator, value)
          })
        });
      });
    }
  });
  return queryFilters;
}

function rootWhereArgToQueryFilters(whereArg: any) {
  return Object.keys(whereArg).reduce((filters, cubeName) => (
    [...filters, ...whereArgToQueryFilters(whereArg[cubeName], capitalize(cubeName))]
  ), [] as any[]);
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
    Granularity
  ];

  metaConfig.forEach(cube => {
    types.push(objectType({
      name: `${cube.config.name}Members`,
      definition(t) {
        cube.config.measures.forEach(measure => {
          if (measure.isVisible) {
            t.field(safeName(measure.name), {
              type: mapType(measure.type),
              description: measure.description
            });
          }
        });
        cube.config.dimensions.forEach(dimension => {
          if (dimension.isVisible) {
            t.field(safeName(dimension.name), {
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
              type: `${mapType(measure.type)}Filter`,
            });
          }
        });
        cube.config.dimensions.forEach(dimension => {
          if (dimension.isVisible) {
            t.field(safeName(dimension.name), {
              type: `${mapType(dimension.type)}Filter`,
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

    if (cube.config.dimensions.filter(dimension => dimension.isVisible).length > 0) {
      types.push(inputObjectType({
        name: `${cube.config.name}GranularityInput`,
        definition(t) {
          cube.config.dimensions.forEach(dimension => {
            if (dimension.isVisible && dimension.type === 'time') {
              t.field(safeName(dimension.name), {
                type: 'Granularity',
              });
            }
          });
        }
      }));
    }
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
            ...(cube.config.dimensions.filter(dimension => dimension.isVisible).length > 0 && {
              granularity: arg({
                type: `${cube.config.name}GranularityInput`
              })
            })
          }
        });
      });
    }
  }));

  types.push(extendType({
    type: 'Query',
    definition(t) {
      t.nonNull.field('load', {
        type: list(nonNull('Result')),
        args: {
          where: arg({
            type: 'RootWhereInput'
          }),
          limit: intArg(),
          offset: intArg(),
          timezone: stringArg(),
          renewQuery: booleanArg(),
        },
        resolve: async (parent, { where, limit, offset, timezone, renewQuery }, context, infos) => {
          const measures: string[] = [];
          const dimensions: string[] = [];
          const timeDimensions: any[] = [];
          let filters: any[] = [];
          const order: Record<string, string> = {};

          if (where) {
            filters = [...filters, ...rootWhereArgToQueryFilters(where)];
          }

          getFieldNodeChildren(infos.fieldNodes[0], infos).forEach(node => {
            const cubeName = capitalize(node.name.value);
            const orderByArg = getArgumentValue(node, 'orderBy');
            if (orderByArg) {
              Object.keys(orderByArg).forEach(key => {
                order[`${cubeName}.${key}`] = orderByArg[key];
              });
            }

            const whereArg = getArgumentValue(node, 'where');
            if (whereArg) {
              filters = [...filters, ...whereArgToQueryFilters(whereArg, cubeName)];
            }

            getFieldNodeChildren(node, infos).forEach(childNode => {
              const memberName = childNode.name.value;
              const memberType = getMemberType(context.metaConfig, cubeName, memberName);

              if (memberType === 'measure') {
                measures.push(`${cubeName}.${memberName}`);
              } else if (memberType === 'dimension') {
                const granularity = getArgumentValue(node, 'granularity');
                if (granularity && Object.keys(granularity).includes(memberName)) {
                  timeDimensions.push({
                    dimension: `${cubeName}.${memberName}`,
                    granularity: granularity[memberName]
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
              await context.apiGateway.load({
                query,
                queryType: QUERY_TYPE.REGULAR_QUERY,
                context: context.req.context,
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
              const path = pair[0].split('.');
              path[0] = unCapitalize(path[0]);
              return R.set(R.lensPath(path), pair[1], res);
            }, {}));
        }
      });
    }
  }));

  return nexusMakeSchema({ types });
}
