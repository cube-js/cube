import { Meta, Query } from '@cubejs-client/core';
import * as t from 'graphql/language';

import { unCapitalize } from './uncapitalize';
import { uniqArray } from './uniq-array';

export type MemberTypeMap = Record<string, 'time' | 'number' | 'string'>;

const API_SUFFIX = '/graphql-to-json';

export async function convertGraphQLToJsonQuery({
  query,
  apiUrl,
  apiToken,
}: {
  query: string;
  apiUrl: string;
  apiToken?: string | null;
}) {
  if (!query.trim() || !apiUrl) {
    return '{}';
  }

  const r = await fetch(apiUrl + API_SUFFIX, {
    method: 'POST',
    headers: {
      Authorization: apiToken || '',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query }),
  });

  const json = await r.json();

  return json.jsonQuery ? JSON.stringify(json.jsonQuery, null, 2) : '{}';
}

function metaToTypes(meta: Meta) {
  const types: MemberTypeMap = {};

  Object.values(meta.cubesMap).forEach((membersByType) => {
    Object.values(membersByType).forEach((members) => {
      Object.values<any>(members).forEach(({ name, type }) => {
        types[name] = type;
      });
    });
  });

  return types;
}

export function convertJsonQueryToGraphQL({ meta, query }: { meta?: Meta | null; query: Query }) {
  const types = meta ? metaToTypes(meta) : null;

  if (!types) {
    return '';
  }

  const converter = new CubeGraphQLConverter(query, types);

  return converter.convert();
}

const singleValueOperators = ['gt', 'gte', 'lt', 'lte'];

const OPERATORS_MAP = {
  equals: 'in',
  notEquals: 'notIn',
  notSet: 'set',
} as const;

enum FilterKind {
  AND = 'AND',
  OR = 'OR',
  PLAIN = 'PLAIN',
}

function baseCubeQuery(
  args: t.ArgumentNode[],
  members: t.FieldNode[],
  name: string = 'CubeQuery'
): t.DocumentNode {
  return {
    kind: t.Kind.DOCUMENT,
    definitions: [
      {
        kind: t.Kind.OPERATION_DEFINITION,
        name: {
          kind: t.Kind.NAME,
          value: name,
        },
        operation: 'query',
        selectionSet: {
          kind: t.Kind.SELECTION_SET,
          selections: [
            {
              kind: t.Kind.FIELD,
              name: {
                kind: t.Kind.NAME,
                value: 'cube',
              },
              arguments: args,
              selectionSet: {
                kind: t.Kind.SELECTION_SET,
                selections: members,
              },
            },
          ],
        },
      },
    ],
  };
}

type CubeField = {
  name: string;
  granularities?: string[];
};

type OrderBy = [string, 'asc' | 'desc'][];

type Cube = {
  fields: CubeField[];
  filters: any;
  orderBy: OrderBy;
};

type Cubes = {
  [cubeName: string]: Cube;
};

export class CubeGraphQLConverter {
  private cubes: Cubes = {};

  public constructor(
    private readonly cubeQuery: Record<string, any>,
    private readonly types: MemberTypeMap
  ) {
    this.prepareCubes();
  }

  public convert() {
    return t.print(baseCubeQuery(this.getCubeArgs(), this.getFieldsSelections()));
  }

  private resolveFilter(
    filter: Record<string, any> | Record<string, any>[],
    parent?: any
  ): (
    | t.ObjectValueNode
    | t.ObjectFieldNode
    | t.ObjectFieldNode[]
    | {
        kind: 'ObjectField';
        name: { kind: 'Name'; value: string };
        value: { kind: 'ObjectValue'; fields: t.ObjectFieldNode[] };
      }
  )[] {
    const plainFilters = Object.values(
      filter.reduce((memo: any, f: any) => {
        if (f.or || f.and) {
          return memo;
        }

        const [cubeName, field] = (f.member || f.dimension).split('.');
        if (!memo[cubeName]) {
          memo[cubeName] = {
            kind: FilterKind.PLAIN,
            cubeName,
            fields: [],
            filters: [],
          };
        }

        memo[cubeName].fields.push(field);
        memo[cubeName].filters.push(f);

        return memo;
      }, {})
    );

    const booleanFilters = filter
      .map((f: any) => {
        if (f.and || f.or) {
          return {
            kind: f.and ? FilterKind.AND : FilterKind.OR,
            filters: f.and || f.or,
          };
        }

        return false;
      })
      .filter(Boolean);

    const groupedFilters: any[] = plainFilters.concat(booleanFilters);

    return groupedFilters.map((item) => {
      if (item.kind !== FilterKind.PLAIN) {
        if (parent) {
          return this.objectValue([
            this.booleanFilter(
              item.kind,
              this.resolveFilter(item.filters, item) as t.ObjectValueNode[]
            ),
          ]);
        }

        return this.booleanFilter(
          item.kind,
          this.resolveFilter(item.filters, item) as t.ObjectValueNode[]
        );
      }

      if (item.fields.length === uniqArray(item.fields).length) {
        if (parent) {
          return this.objectValue(
            this.objectFieldFilter(item.filters),
            unCapitalize(item.cubeName)
          );
        }

        return this.objectFieldFilter(item.filters, unCapitalize(item.cubeName));
      } else {
        return this.objectField(
          this.booleanFilter(
            parent?.kind || 'AND',
            item.filters.map((f: any) => {
              if (f.and || f.or) {
                this.resolveFilter(item.filters, item.kind);
              }

              return this.objectValue(this.objectFieldFilter(f));
            })
          ),
          unCapitalize(item.cubeName)
        );
      }
    });
  }

  private objectValue(
    fields: t.ObjectFieldNode | t.ObjectFieldNode[],
    fieldName?: string
  ): t.ObjectValueNode {
    if (fieldName) {
      return {
        kind: t.Kind.OBJECT,
        fields: [
          {
            kind: t.Kind.OBJECT_FIELD,
            name: {
              kind: t.Kind.NAME,
              value: fieldName,
            },
            value: {
              kind: t.Kind.OBJECT,
              fields: Array.isArray(fields) ? fields : [fields],
            },
          },
        ],
      };
    }

    return {
      kind: t.Kind.OBJECT,
      fields: Array.isArray(fields) ? fields : [fields],
    };
  }

  private objectFieldFilter(
    filter: Record<string, any> | Record<string, any>[],
    parentName?: string
  ): t.ObjectFieldNode[] {
    const filters = Array.isArray(filter) ? filter : [filter];

    const value = (f: any): t.ValueNode => {
      const kind = this.types[f.member || f.dimension] === 'number' ? t.Kind.FLOAT : t.Kind.STRING;

      if (['set', 'notSet'].includes(f.operator)) {
        return {
          kind: t.Kind.BOOLEAN,
          value: f.operator === 'set',
        };
      }

      if (typeof f.values === 'string') {
        return {
          kind,
          value: f.values,
        };
      }

      if (f.values.length === 1) {
        return {
          kind,
          value: f.values[0].toString(),
        };
      }

      return {
        kind: t.Kind.LIST,
        values: f.values.map((v: any) => ({
          kind,
          value: v,
        })),
      };
    };

    const fields = filters.map<t.ObjectFieldNode>((f: any) => {
      const memberName = f.member || f.dimension;
      if (!this.types[memberName]) {
        throw new Error(
          `Unknown member type for "${memberName}". Make sure "${memberName}" exists in the data model.`
        );
      }

      if (singleValueOperators.includes(f.operator) && (f.values || []).length > 1) {
        throw new Error(`Filter operator "${f.operator}" must have a single value`);
      }

      return {
        kind: t.Kind.OBJECT_FIELD,
        name: {
          kind: t.Kind.NAME,
          value: memberName.split('.')[1],
        },
        value: {
          kind: t.Kind.OBJECT,
          fields:
            f.values === undefined && !['set', 'notSet'].includes(f.operator)
              ? []
              : [
                  {
                    kind: t.Kind.OBJECT_FIELD,
                    name: {
                      kind: t.Kind.NAME,
                      // A single value maps to "equals"
                      // Whereas multiple values for "equals" operator maps to "in"
                      // value: operatorsMap[f.operator] || f.operator,
                      value:
                        f.operator === 'equals' && (f.values || []).length <= 1
                          ? 'equals'
                          : f.operator in OPERATORS_MAP
                            ? OPERATORS_MAP[f.operator as keyof typeof OPERATORS_MAP]
                            : f.operator,
                    },
                    value: value(f),
                  },
                ],
        },
      } as t.ObjectFieldNode;
    });

    if (parentName) {
      return [
        {
          kind: t.Kind.OBJECT_FIELD,
          name: {
            kind: t.Kind.NAME,
            value: parentName,
          },
          value: {
            kind: t.Kind.OBJECT,
            fields,
          },
        },
      ];
    }

    return fields;
  }

  private objectField(fields: t.ObjectFieldNode | t.ObjectFieldNode[], fieldName: string) {
    return {
      kind: t.Kind.OBJECT_FIELD,
      name: {
        kind: t.Kind.NAME,
        value: fieldName,
      },
      value: {
        kind: t.Kind.OBJECT,
        fields: Array.isArray(fields) ? fields : [fields],
      },
    };
  }

  // OR: [{ orders: { status: { equals: "active"} }}]
  private booleanFilter(kind: FilterKind, values: t.ObjectValueNode[]): t.ObjectFieldNode {
    return {
      kind: t.Kind.OBJECT_FIELD,
      name: {
        kind: t.Kind.NAME,
        value: kind,
      },
      value: {
        kind: t.Kind.LIST,
        values,
      },
    };
  }

  private orderByArg(orderBy: OrderBy): t.ArgumentNode {
    return {
      kind: t.Kind.ARGUMENT,
      name: {
        kind: t.Kind.NAME,
        value: 'orderBy',
      },
      value: {
        kind: t.Kind.OBJECT,
        fields: orderBy.map(([field, value]) => ({
          kind: t.Kind.OBJECT_FIELD,
          name: {
            kind: t.Kind.NAME,
            value: field,
          },
          value: {
            kind: t.Kind.ENUM,
            value,
          },
        })),
      },
    };
  }

  private getFieldsSelections() {
    const selections: t.FieldNode[] = [];

    Object.entries(this.cubes).forEach(([cubeName, { fields, orderBy }]) => {
      selections.push({
        kind: t.Kind.FIELD,
        name: {
          kind: t.Kind.NAME,
          value: cubeName,
        },
        arguments: orderBy.length ? [this.orderByArg(orderBy)] : [],
        selectionSet: {
          kind: t.Kind.SELECTION_SET,
          selections: fields.map((field) => {
            let granularitySelection: Partial<t.FieldNode> | null = null;

            if (field.granularities) {
              granularitySelection = {
                selectionSet: {
                  kind: t.Kind.SELECTION_SET,
                  selections: field.granularities.map((value) => ({
                    kind: t.Kind.FIELD,
                    name: {
                      kind: t.Kind.NAME,
                      value,
                    },
                  })),
                },
              };
            }

            return {
              kind: t.Kind.FIELD,
              name: {
                kind: t.Kind.NAME,
                value: field.name,
              },
              ...granularitySelection,
            };
          }),
        },
      });
    });

    return selections;
  }

  private getCubeArgs() {
    const cubeArgsKeys: [
      string,
      typeof t.Kind.STRING | typeof t.Kind.INT | typeof t.Kind.OBJECT,
    ][] = [
      ['timezone', t.Kind.STRING],
      ['limit', t.Kind.INT],
      ['offset', t.Kind.INT],
    ];

    const cubeArgs: t.ArgumentNode[] = [];

    cubeArgsKeys.forEach(([key, kind]) => {
      if (this.cubeQuery[key]) {
        cubeArgs.push({
          kind: t.Kind.ARGUMENT,
          name: {
            kind: t.Kind.NAME,
            value: key,
          },
          value: {
            kind: <typeof t.Kind.STRING | typeof t.Kind.INT>kind,
            value: this.cubeQuery[key],
          },
        });
      }
    });

    const filters = [...(this.cubeQuery.filters || [])];

    (this.cubeQuery.timeDimensions || []).forEach((td: any) => {
      if (td.dateRange) {
        filters.push({
          member: td.dimension,
          operator: 'inDateRange',
          values: td.dateRange,
        });
      }
    });

    if (filters.length) {
      cubeArgs.push({
        kind: t.Kind.ARGUMENT,
        name: {
          kind: t.Kind.NAME,
          value: 'where',
        },
        value: {
          kind: t.Kind.OBJECT,
          fields: this.resolveFilter(filters) as t.ObjectFieldNode[],
        },
      });
    }

    return cubeArgs;
  }

  private prepareCubes() {
    const initCube = (cubeName: string) => {
      if (!this.cubes[cubeName]) {
        this.cubes[cubeName] = {
          fields: [],
          filters: [],
          orderBy: [],
        };
      }
    };

    ['measures', 'dimensions', 'segments'].forEach((key) => {
      if (!this.cubeQuery[key]) {
        return;
      }

      this.cubeQuery[key].forEach((value: string) => {
        const [name, field, granularity] = value.split('.');
        const cubeName = unCapitalize(name);

        let gqlGranularity = granularity;
        if (this.types[`${name}.${field}`] === 'time') {
          gqlGranularity = 'value';
        }

        initCube(cubeName);

        // eslint-disable-next-line
        const currentField = this.cubes[cubeName].fields.find(({ name }) => name === field);

        this.cubes[cubeName].fields.push({
          name: field,
          ...(gqlGranularity
            ? {
                granularities: [...(currentField?.granularities || []), gqlGranularity],
              }
            : null),
        });
      });
    });

    const map: Record<string, string[]> = {};
    this.cubeQuery.timeDimensions?.forEach((td: any) => {
      const [name, field] = td.dimension.split('.');
      const cubeFieldName = `${name}.${field}`;
      if (td.granularity) {
        map[cubeFieldName] = (map[cubeFieldName] || []).concat([td.granularity]);
      }
    });

    Object.entries(map).forEach(([cubeField, granularities]) => {
      const [name, field] = cubeField.split('.');
      const cubeName = unCapitalize(name);
      initCube(cubeName);

      const existingField = this.cubes[cubeName].fields.find((f) => f.name === field);

      if (existingField) {
        existingField.granularities = uniqArray([
          ...(existingField.granularities || []),
          ...granularities,
        ]);
      } else {
        this.cubes[cubeName].fields.push({
          name: field,
          granularities,
        });
      }
    });

    if (this.cubeQuery.order) {
      const orderBy = Array.isArray(this.cubeQuery.order)
        ? this.cubeQuery.order
        : Object.entries(this.cubeQuery.order);

      orderBy.forEach(([key, order]) => {
        const [cubeName, member] = key.split('.');
        const gqlCubeName = unCapitalize(cubeName);

        if (!this.cubes[gqlCubeName]) {
          throw new Error(
            `Order without selecting the cube is not allowed. Did you forget to include the "${cubeName}" cube?`
          );
        }

        const exists = this.cubes[gqlCubeName].fields.find(({ name }) => name === member);

        if (!exists) {
          throw new Error(
            `Order without selecting the member is not allowed. Did you forget to include the "${member}" member?`
          );
        }
        this.cubes[gqlCubeName].orderBy.push([member, order]);
      });
    }
  }
}
