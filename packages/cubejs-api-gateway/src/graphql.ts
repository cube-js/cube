import R from 'ramda';
import moment from 'moment-timezone';

import {
  GraphQLResolveInfo,
  ArgumentNode,
  DirectiveNode,
  FieldNode,
  VariableNode,
  ValueNode,
  GraphQLSchema,
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
import { camelize } from 'inflection';
import {
  DateTimeResolver,
} from 'graphql-scalars';

import { QueryType, MemberType } from './types/enums';

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
    t.list.string('contains');
    t.list.string('notContains');
    t.list.string('startsWith');
    t.list.string('notStartsWith');
    t.list.string('notContains');
    t.list.string('endsWith');
    t.list.string('notEndsWith');
    t.boolean('set');
  }
});

const DateTimeFilter = inputObjectType({
  name: 'DateTimeFilter',
  definition(t) {
    t.list.string('equals');
    t.list.string('notEquals');
    t.list.string('in');
    t.list.string('notIn');
    t.list.string('inDateRange');
    t.list.string('notInDateRange');
    t.string('beforeDate');
    t.string('beforeOrOnDate');
    t.string('afterDate');
    t.string('afterOrOnDate');
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

function mapWhereValue(operator: string, value: any): undefined | string | string[] {
  value = Array.isArray(value) ? value.map(v => v.toString()) : [value.toString()];

  if (operator === 'set') {
    return undefined;
  } else if (['inDateRange', 'notInDateRange'].includes(operator)) {
    if (value.length === 1 && !/^\d\d\d\d-\d\d-\d\d/.test(value[0])) {
      return value[0].toString();
    }
  }

  return value;
}

function safeName(name: string) {
  return name.split('.').slice(1).join('');
}

function objectName(name: string) {
  return camelize(name, false);
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

function getArgumentValue(node: FieldNode, argName: string, variables: Record<string, any> = {}) {
  const argument = node.arguments?.find(a => a.name.value === argName)?.value;

  if (argument?.kind === 'Variable') {
    const varValue = variables[argument.name.value];
    if (varValue === undefined) {
      throw new Error(`Variable "${argument.name.value}" is not defined`);
    }
    return variables[argument.name.value];
  }

  return argument ? parseArgumentValue(argument) : argument;
}

function getMemberType(metaConfig: any, cubeName: string, memberName: string) {
  const cubeConfig = metaConfig.find(cube => (cube.config.name === cubeName) || cube.config.name === capitalize(cubeName));
  if (!cubeConfig) return undefined;

  return [MemberType.MEASURES, MemberType.DIMENSIONS].find((memberType) => (cubeConfig.config[memberType]
    .findIndex(entry => entry.name === `${cubeName}.${memberName}` || entry.name === `${capitalize(cubeName)}.${memberName}`) !== -1
  ));
}

function whereArgToQueryFilters(
  whereArg: Record<string, any>,
  prefix?: string,
  metaConfig: any[] = []
) {
  const queryFilters: any[] = [];
  
  Object.keys(whereArg).forEach((key) => {
    const cubeExists = metaConfig.find((cube) => cube.config.name === key);
    const normalizedKey = cubeExists ? key : capitalize(key);
    
    if (['OR', 'AND'].includes(key)) {
      queryFilters.push({
        [key.toLowerCase()]: whereArg[key].reduce(
          (filters, whereBooleanArg) => [
            ...filters,
            ...whereArgToQueryFilters(whereBooleanArg, prefix, metaConfig),
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
            normalizedKey,
            metaConfig
          )
        );
      } else {
        const res = whereArgToQueryFilters(whereArg[key], normalizedKey, metaConfig);

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
              : `${normalizedKey}.${member}`,
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

export function makeSchema(metaConfig: any): GraphQLSchema {
  const types: any[] = [
    DateTimeScalar,
    FloatFilter,
    StringFilter,
    DateTimeFilter,
    OrderBy,
    TimeDimension
  ];

  function hasMembers(cube: any) {
    if (cube.public === false) {
      return false;
    }
    
    return ([...cube.config.measures, ...cube.config.dimensions].filter((member) => member.isVisible)).length > 0;
  }

  metaConfig.forEach(cube => {
    if (hasMembers(cube)) {
      types.push(objectType({
        name: `${objectName(cube.config.name)}Members`,
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
        name: `${objectName(cube.config.name)}WhereInput`,
        definition(t) {
          t.field('AND', { type: list(nonNull(`${objectName(cube.config.name)}WhereInput`)) });
          t.field('OR', { type: list(nonNull(`${objectName(cube.config.name)}WhereInput`)) });
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
        name: `${objectName(cube.config.name)}OrderByInput`,
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
    }
  });

  types.push(inputObjectType({
    name: 'RootWhereInput',
    definition(t) {
      t.field('AND', { type: list(nonNull('RootWhereInput')) });
      t.field('OR', { type: list(nonNull('RootWhereInput')) });
      metaConfig.forEach(cube => {
        if (hasMembers(cube)) {
          t.field(unCapitalize(cube.config.name), {
            type: `${objectName(cube.config.name)}WhereInput`
          });
        }
      });
    }
  }));

  types.push(inputObjectType({
    name: 'RootOrderByInput',
    definition(t) {
      metaConfig.forEach(cube => {
        if (hasMembers(cube)) {
          t.field(unCapitalize(cube.config.name), {
            type: `${objectName(cube.config.name)}OrderByInput`
          });
        }
      });
    }
  }));

  types.push(objectType({
    name: 'Result',
    definition(t) {
      metaConfig.forEach(cube => {
        if (hasMembers(cube)) {
          t.nonNull.field(unCapitalize(cube.config.name), {
            type: `${objectName(cube.config.name)}Members`,
            args: {
              where: arg({
                type: `${objectName(cube.config.name)}WhereInput`
              }),
              orderBy: arg({
                type: `${objectName(cube.config.name)}OrderByInput`
              }),
            }
          });
        }
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
            filters = whereArgToQueryFilters(where, undefined, metaConfig);
          }

          if (orderBy) {
            Object.entries<any>(orderBy).forEach(([cubeName, members]) => {
              Object.entries<any>(members).forEach(([member, value]) => {
                order.push([`${capitalize(cubeName)}.${member}`, value]);
              });
            });
          }

          getFieldNodeChildren(infos.fieldNodes[0], infos).forEach(cubeNode => {
            const cubeExists = metaConfig.find((cube) => cube.config.name === cubeNode.name.value);
            
            const cubeName = cubeExists ? (cubeNode.name.value) : capitalize(cubeNode.name.value);
            const orderByArg = getArgumentValue(cubeNode, 'orderBy', infos.variableValues);
            // todo: throw if both RootOrderByInput and [Cube]OrderByInput provided
            if (orderByArg) {
              Object.keys(orderByArg).forEach(key => {
                order.push([`${cubeName}.${key}`, orderByArg[key]]);
              });
            }

            const whereArg = getArgumentValue(cubeNode, 'where', infos.variableValues);
            if (whereArg) {
              filters = whereArgToQueryFilters(whereArg, cubeName).concat(filters);
            }

            // Push down all inDateRange filters to time dimensions to leverage pre-aggregations
            const dateRangeFilters = {};
            filters = filters.filter((f) => {
              if (f.operator === 'inDateRange' && !dateRangeFilters[f.member]) {
                dateRangeFilters[f.member] = f.values;
                return false;
              }

              return true;
            });

            getFieldNodeChildren(cubeNode, infos).forEach(memberNode => {
              const memberName = memberNode.name.value;
              const memberType = getMemberType(metaConfig, cubeName, memberName);
              const key = `${cubeName}.${memberName}`;

              if (memberType === MemberType.MEASURES) {
                measures.push(key);
              } else if (memberType === MemberType.DIMENSIONS) {
                const granularityNodes = getFieldNodeChildren(memberNode, infos);
                if (granularityNodes.length > 0) {
                  granularityNodes.forEach(granularityNode => {
                    const granularityName = granularityNode.name.value;
                    if (granularityName === 'value') {
                      dimensions.push(key);
                    } else {
                      timeDimensions.push({
                        dimension: key,
                        granularity: granularityName,
                        ...(dateRangeFilters[key] ? {
                          dateRange: dateRangeFilters[key],
                        } : null)
                      });
                    }
                  });
                } else {
                  dimensions.push(`${cubeName}.${memberName}`);
                }
              }
            });

            if (Object.keys(dateRangeFilters).length && !timeDimensions.length) {
              Object.entries(dateRangeFilters).forEach(([dimension, dateRange]) => {
                timeDimensions.push({
                  dimension,
                  dateRange
                });
              });
            }
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

          const results = await new Promise<any>((resolve, reject) => {
            apiGateway.load({
              query,
              queryType: QueryType.REGULAR_QUERY,
              context: req.context,
              res: (message) => {
                if (message.error) {
                  reject(new Error(message.error));
                }
                resolve(message);
              },
              apiType: 'graphql',
            }).catch(reject);
          });

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
