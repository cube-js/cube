import * as t from 'graphql/language';
import { uniq } from 'ramda';

import { MemberType, unCapitalize } from './utils';

const singleValueOperators = ['gt', 'gte', 'lt', 'lte'];

const operatorsMap = {
  equals: 'in',
  notEquals: 'notIn',
  notSet: 'set',
};

enum FilterKind {
  AND = 'AND',
  OR = 'OR',
  PLAIN = 'PLAIN',
}

function baseCubeQuery(args: t.ArgumentNode[], members: t.FieldNode[], name: string = 'CubeQuery'): t.DocumentNode {
  return {
    kind: t.Kind.DOCUMENT,
    definitions: [
      {
        kind: t.Kind.OPERATION_DEFINITION,
        name: {
          kind: t.Kind.NAME,
          value: name,
        },
        operation: t.OperationTypeNode.QUERY,
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

type Cube = {
  fields: CubeField[];
  filters: any;
};

type Cubes = {
  [cubeName: string]: Cube;
};

export class CubeGraphQLConverter {
  private cubes: Cubes = {};

  public constructor(private readonly cubeQuery: Record<string, any>, private readonly types: MemberType) {
    this.prepareCubes();
  }

  public convert() {
    return t.print(baseCubeQuery(this.getCubeArgs(), this.getFieldsSelections()));
  }

  private resolveFilter(filter: Record<string, any> | Record<string, any>[], parent?: any) {
    const plainFilters = Object.values(
      filter.reduce((memo, f) => {
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
      .map((f) => {
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
          return this.objectValue([this.booleanFilter(item.kind, this.resolveFilter(item.filters, item))]);
        }

        return this.booleanFilter(item.kind, this.resolveFilter(item.filters, item));
      }

      if (item.fields.length === uniq(item.fields).length) {
        if (parent) {
          return this.objectValue(this.objectFieldFilter(item.filters), unCapitalize(item.cubeName));
        }

        return this.objectFieldFilter(item.filters, unCapitalize(item.cubeName));
      } else {
        return this.objectField(
          this.booleanFilter(
            parent?.kind || 'AND',
            item.filters.map((f) => {
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

  private objectValue(fields: t.ObjectFieldNode | t.ObjectFieldNode[], filedName?: string): t.ObjectValueNode {
    if (filedName) {
      return {
        kind: t.Kind.OBJECT,
        fields: [
          {
            kind: t.Kind.OBJECT_FIELD,
            name: {
              kind: t.Kind.NAME,
              value: filedName,
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

    const value = (f): t.ValueNode => {
      const kind = this.types[f.member || f.dimension] === 'number' ? t.Kind.FLOAT : t.Kind.STRING;

      if (['set', 'notSet'].includes(f.operator)) {
        return {
          kind: t.Kind.BOOLEAN,
          value: f.operator === 'set',
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
        values: f.values.map((v) => ({
          kind,
          value: v,
        })),
      };
    };

    const fields = filters.map<t.ObjectFieldNode>((f) => {
      const memberName = f.member || f.dimension;
      if (!this.types[memberName]) {
        throw new Error(`Unknown member type for "${memberName}". Make sure "${memberName}" exists in the schema.`);
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
          fields: [
            {
              kind: t.Kind.OBJECT_FIELD,
              name: {
                kind: t.Kind.NAME,
                value: !f.values || (f.values || []).length > 1 ? operatorsMap[f.operator] || f.operator : f.operator,
              },
              value: value(f),
            },
          ],
        },
      };
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

  private getFieldsSelections() {
    const selections: t.FieldNode[] = [];

    Object.entries(this.cubes).forEach(([cubeName, { fields }]) => {
      selections.push({
        kind: t.Kind.FIELD,
        name: {
          kind: t.Kind.NAME,
          value: cubeName,
        },
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
    const cubeArgsKeys: [string, t.Kind.STRING | t.Kind.INT | t.Kind.OBJECT][] = [
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
            kind: <t.Kind.STRING | t.Kind.INT>kind,
            value: this.cubeQuery[key],
          },
        });
      }
    });

    const filters = this.cubeQuery.filters || [];

    (this.cubeQuery.timeDimensions || []).forEach((td) => {
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
          fields: this.resolveFilter(filters),
        },
      });
    }

    return cubeArgs;
  }

  private prepareCubes() {
    const initCube = (cubeName) => {
      if (!this.cubes[cubeName]) {
        this.cubes[cubeName] = {
          fields: [],
          filters: [],
        };
      }
    };

    ['measures', 'dimensions', 'segments'].forEach((key) => {
      if (!this.cubeQuery[key]) {
        return;
      }

      this.cubeQuery[key].forEach((value) => {
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
    this.cubeQuery.timeDimensions?.forEach((td) => {
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

      this.cubes[cubeName].fields.push({
        name: field,
        granularities,
      });
    });
  }
}
