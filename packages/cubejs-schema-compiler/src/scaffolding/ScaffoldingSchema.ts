import inflection from 'inflection';
import R from 'ramda';
import { notEmpty } from '@cubejs-backend/shared';
import { UserError } from '../compiler';
import { toIdentifier, toSnakeCase } from './utils';

enum ColumnType {
  Time = 'time',
  Number = 'number',
  String = 'string',
  Boolean = 'boolean',
}

export enum MemberType {
  Measure = 'measure',
  Dimension = 'dimension',
  None = 'none',
}

export type Dimension = {
  name: string;
  types: any[];
  title: string;
  isPrimaryKey?: boolean;
  type?: any;
};

export type TableName = string | [string, string];

export type JoinRelationship = 'hasOne' | 'hasMany' | 'belongsTo';

type ColumnsToJoin = {
  cubeToJoin: string;
  columnToJoin: string;
  tableName: TableName;
};

export type CubeDescriptorMember = {
  name: string;
  title: string;
  memberType: MemberType;
  type?: string;
  types: string[];
  isId?: boolean;
  included?: boolean;
  isPrimaryKey?: boolean;
};

export type Join = {
  thisTableColumn: string;
  thisTableColumnIncludedAsDimension?: boolean;
  tableName: TableName;
  cubeToJoin: string;
  columnToJoin: string;
  columnToJoinIncludedAsDimension?: boolean;
  relationship: JoinRelationship;
};

export type CubeDescriptor = {
  cube: string;
  tableName: TableName;
  table: string;
  schema: string;
  members: CubeDescriptorMember[];
  joins: Join[];
};

export type TableSchema = {
  cube: string;
  tableName: TableName;
  schema: any;
  table: any;
  measures: any[];
  dimensions: Dimension[];
  drillMembers?: Dimension[];
  joins: Join[];
};

const MEASURE_DICTIONARY = [
  'amount',
  'price',
  'count',
  'balance',
  'total',
  'number',
  'cost',
  'qty',
  'quantity',
  'duration',
  'value',
];

const idRegex = '_id$|id$';

type ForeignKey = {
  // eslint-disable-next-line camelcase
  target_table: string;
  // eslint-disable-next-line camelcase
  target_column: string;
};

type ColumnData = {
  name: string,
  type: string,
  attributes: string[],
  // eslint-disable-next-line camelcase
  foreign_keys?: ForeignKey[],
};

export type DatabaseSchema = Record<string, { [key: string]: ColumnData[] }>;

type TableData = {
  schema: string,
  table: string,
  tableName: TableName;
  tableDefinition: ColumnData[],
};

type ScaffoldingSchemaOptions = {
  includeNonDictionaryMeasures?: boolean;
  snakeCase?: boolean;
};

export class ScaffoldingSchema {
  // Table names like `constructor` or `__proto__` must not resolve to Object.prototype
  private tableNamesToTables: Map<string, TableData[]> = new Map();

  public constructor(
    private readonly dbSchema: DatabaseSchema,
    private readonly options: ScaffoldingSchemaOptions = {}
  ) {}

  public resolveTableName(tableName: TableName) {
    let tableParts;
    if (Array.isArray(tableName)) {
      tableParts = tableName;
    } else {
      tableParts = tableName.match(/(["`].*?["`]|[^`".]+)+(?=\s*|\s*$)/g)!;
    }

    if (tableParts.length === 2) {
      this.resolveTableDefinition(tableName);
      return tableName;
    } else if (tableParts.length === 1 && typeof tableName === 'string') {
      const schema = Object.keys(this.dbSchema).find(
        (tableSchema) => this.dbSchema[tableSchema][tableName] ||
          this.dbSchema[tableSchema][inflection.tableize(tableName)]
      );
      if (!schema) {
        throw new UserError(`Can't find any table with '${tableName}' name`);
      }
      if (this.dbSchema[schema][tableName]) {
        return `${schema}.${tableName}`;
      }
      if (this.dbSchema[schema][inflection.tableize(tableName)]) {
        return `${schema}.${inflection.tableize(tableName)}`;
      }
    }

    throw new UserError(
      'Table names should be in <table> or <schema>.<table> format'
    );
  }

  public cubeDescriptors(tableNames: TableName[]): CubeDescriptor[] {
    const cubes = this.generateForTables(tableNames);

    function member(type: MemberType) {
      return (value: Omit<CubeDescriptorMember, 'memberType'>) => ({
        memberType: type,
        ...R.pick(['name', 'title', 'types', 'isPrimaryKey', 'included', 'isId'], value)
      });
    }

    return cubes.map((cube) => ({
      cube: cube.cube,
      tableName: cube.tableName,
      table: cube.table,
      schema: cube.schema,
      members: (cube.measures || []).map(member(MemberType.Measure))
        .concat((cube.dimensions || []).map(member(MemberType.Dimension))),
      joins: cube.joins
    }));
  }

  public generateForTables(tableNames: TableName[]) {
    this.prepareTableNamesToTables(tableNames);
    return tableNames.map(tableName => this.tableSchema(tableName, true));
  }

  protected prepareTableNamesToTables(tableNames: TableName[]) {
    const nameToDefinitions = tableNames.flatMap(tableName => {
      const [schema, table] = this.parseTableName(tableName);
      const tableDefinition = this.resolveTableDefinition(tableName);
      const definition: TableData = {
        schema, table, tableDefinition, tableName
      };
      const tableizeName = inflection.tableize(this.fixCase(table));
      const parts = tableizeName.split('_');
      const tableNamesFromParts = R.range(0, parts.length - 1).map(toDrop => inflection.tableize(R.drop(toDrop, parts).join('_')));
      const names = R.uniq([table, tableizeName].concat(tableNamesFromParts));
      return names.map((n): [string, TableData] => [n, definition]);
    });

    this.tableNamesToTables = new Map();

    for (const [name, definition] of nameToDefinitions) {
      const definitions = this.tableNamesToTables.get(name);
      if (definitions) {
        definitions.push(definition);
      } else {
        this.tableNamesToTables.set(name, [definition]);
      }
    }
  }

  public resolveTableDefinition(tableName: TableName) {
    const [schema, table] = this.parseTableName(tableName);
    if (!this.dbSchema[schema]) {
      throw new UserError(`Can't resolve ${tableName}: '${schema}' does not exist`);
    }
    if (!this.dbSchema[schema][table]) {
      throw new UserError(`Can't resolve ${tableName}: '${table}' does not exist`);
    }
    return this.dbSchema[schema][table];
  }

  protected tableSchema(tableName: TableName, includeJoins: boolean): TableSchema {
    const [schema, table] = this.parseTableName(tableName);
    const tableDefinition = this.resolveTableDefinition(tableName);
    const dimensions = this.dimensions(tableDefinition);

    return {
      cube: this.cubeName(table),
      tableName,
      schema,
      table,
      measures: this.numberMeasures(tableDefinition),
      dimensions,
      joins: includeJoins ? this.joins(tableName, tableDefinition) : []
    };
  }

  protected parseTableName(tableName: TableName): [string, string] {
    let schemaAndTable;
    if (Array.isArray(tableName)) {
      schemaAndTable = tableName;
    } else {
      schemaAndTable = tableName.match(/(["`].*?["`]|[^`".]+)+(?=\s*|\s*$)/g)!;
    }
    if (schemaAndTable.length !== 2) {
      throw new UserError(`Incorrect format for '${tableName}'. Should be in '<schema>.<table>' format`);
    }
    return schemaAndTable as [string, string];
  }

  protected dimensions(tableDefinition: ColumnData[]): Dimension[] {
    return this.dimensionColumns(tableDefinition).map(column => {
      const res: Dimension = {
        name: column.name,
        types: [column.columnType || this.columnType(column)],
        title: inflection.titleize(column.name),
      };

      if (column.columnType !== 'time') {
        res.isPrimaryKey = column.attributes?.includes('primaryKey') ||
          this.fixCase(column.name) === 'id';
      }
      return res;
    });
  }

  protected numberMeasures(tableDefinition: ColumnData[]) {
    return tableDefinition.filter(
      column => (!column.name.startsWith('_') &&
        (this.columnType(column) === 'number') &&
        (this.options.includeNonDictionaryMeasures ? this.fixCase(column.name) !== 'id' : this.fromMeasureDictionary(column)))
    ).map(column => ({
      name: column.name,
      types: ['sum', 'avg', 'min', 'max'],
      title: inflection.titleize(column.name),
      ...(this.options.includeNonDictionaryMeasures ? { included: this.fromMeasureDictionary(column) } : null)
    }));
  }

  protected fromMeasureDictionary(column: ColumnData) {
    return !column.name.match(new RegExp(idRegex, 'i')) && !!MEASURE_DICTIONARY.find(word => this.fixCase(column.name).endsWith(word));
  }

  protected dimensionColumns(tableDefinition: ColumnData[]): Array<ColumnData & { columnType?: string }> {
    const dimensionColumns = tableDefinition.filter(
      column => !column.name.startsWith('_') && ['string', 'boolean'].includes(this.columnType(column)) ||
        column.attributes?.includes('primaryKey') ||
        this.fixCase(column.name) === 'id'
    );

    const timeColumns = R.pipe(
      // @ts-ignore
      R.filter(column => !column.name.startsWith('_') && this.columnType(column) === 'time'),
      R.sortBy((column: ColumnData) => this.timeColumnIndex(column)),
      // @ts-ignore
      R.map(column => ({ ...column, columnType: 'time' })) // TODO do we need it?
      // @ts-ignore
    )(tableDefinition);

    return dimensionColumns.concat(timeColumns);
  }

  private cubeName(table: string) {
    return toIdentifier(
      this.options.snakeCase ? toSnakeCase(table) : inflection.camelize(table.replace(/[^\p{L}\p{N}]+/gu, '_'))
    );
  }

  private fixCase(value: string) {
    if (this.options.snakeCase) {
      return toSnakeCase(value);
    }

    return value.toLocaleLowerCase();
  }

  protected joins(tableName: TableName, tableDefinition: ColumnData[]): Join[] {
    return R.unnest(tableDefinition
      .map(column => {
        let columnsToJoin: ColumnsToJoin[] = [];

        if (column.foreign_keys?.length) {
          column.foreign_keys.forEach(fk => {
            const targetTableDefinition = this.tableNamesToTables.get(fk.target_table)?.find(t => t.table === fk.target_table);
            if (targetTableDefinition) {
              columnsToJoin.push({
                cubeToJoin: this.cubeName(fk.target_table),
                columnToJoin: fk.target_column,
                tableName: targetTableDefinition.tableName
              });
            }
          });
        } else if ((column.name.match(new RegExp(idRegex, 'i')) && this.fixCase(column.name) !== 'id')) {
          const withoutId = column.name.replace(new RegExp(idRegex, 'i'), '');
          const tablesToJoin = this.tableNamesToTables.get(withoutId) ||
          this.tableNamesToTables.get(inflection.tableize(withoutId)) ||
          this.tableNamesToTables.get(this.fixCase(withoutId)) ||
          this.tableNamesToTables.get(inflection.tableize(this.fixCase(withoutId)));

          if (!tablesToJoin) {
            return null;
          }

          columnsToJoin = tablesToJoin.map(definition => {
            if (tableName === definition.tableName) {
              return null;
            }
            let columnForJoin = definition.tableDefinition.find(c => this.fixCase(c.name) === this.fixCase(column.name));
            columnForJoin = columnForJoin || definition.tableDefinition.find(c => this.fixCase(c.name) === 'id');
            if (!columnForJoin) {
              return null;
            }
            return {
              cubeToJoin: this.cubeName(definition.table),
              columnToJoin: columnForJoin.name,
              tableName: definition.tableName
            };
          }).filter(notEmpty);
        }

        if (!columnsToJoin.length) {
          return null;
        }

        return columnsToJoin.map<Join>(columnToJoin => ({
          thisTableColumn: column.name,
          tableName: columnToJoin.tableName,
          cubeToJoin: columnToJoin.cubeToJoin,
          columnToJoin: columnToJoin.columnToJoin,
          relationship: 'belongsTo'
        }));
      })
      .filter(notEmpty));
  }

  protected timeColumnIndex(column: ColumnData): number {
    const name = this.fixCase(column.name);
    if (name.indexOf('create') !== -1) {
      return 0;
    } else if (name.indexOf('update') !== -1) {
      return 1;
    } else {
      return 2;
    }
  }

  protected columnType(column: ColumnData): ColumnType {
    const type = this.fixCase(column.type);

    if (['time', 'date'].find(t => type.includes(t))) {
      return ColumnType.Time;
    } else if ([
      'int', // integer, bigint, smallint, tinyint, mediumint, uint8, uint16, uint32, uint64, uinteger, ubigint, usmallint, hugeint, byteint, etc.
      'dec', // decimal
      'double', // double, double precision
      'numb', // number, numeric, bignumeric
      'float', // float, float4, float8, float32, float64, binary_float
      'real', // real
      'serial', // serial, bigserial, smallserial
      'money', // money, smallmoney
    ].find(t => type.includes(t))) {
      // enums are not Numbers
      return ColumnType.Number;
    } else if (['bool'].find(t => type.includes(t))) {
      return ColumnType.Boolean;
    }

    return ColumnType.String;
  }
}
