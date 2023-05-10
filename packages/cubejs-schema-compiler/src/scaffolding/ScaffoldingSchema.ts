import inflection from 'inflection';
import R from 'ramda';
import { UserError } from '../compiler';

enum ColumnType {
  Time = 'time',
  Number = 'number',
  String = 'string',
}

export enum MemberType {
  Measure = 'measure',
  Dimension = 'dimension',
  None = 'none'
}

export type Dimension = {
  name: string;
  types: any[];
  title: string;
  isPrimaryKey?: boolean;
  type?: any;
};

export type TableName = string | [string, string];

type JoinRelationship = 'hasOne' | 'hasMany' | 'belongsTo';

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

type Join = {
  thisTableColumn: string;
  tableName: TableName;
  cubeToJoin: string;
  columnToJoin: string;
  relationship: JoinRelationship;
};

export type CubeDescriptor = {
  cube: string,
  tableName: TableName,
  table: string
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
  joins: any[];
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
  'value'
];

const DRILL_MEMBERS_DICTIONARY = [
  'id',
  'name',
  'title',
  'firstname',
  'first_name',
  'lastname',
  'last_name',
  'createdat',
  'created_at',
  'created',
  'timestamp',
  'city',
  'country',
  'date'
];

const idRegex = '_id$|id$';

export type DatabaseSchema = Record<string, Record<string, any>>;

type ScaffoldingSchemaOptions = {
  includeNonDictionaryMeasures?: boolean;
};

export class ScaffoldingSchema {
  private tableNamesToTables: any;

  public constructor(
    private readonly dbSchema: DatabaseSchema,
    private readonly options: ScaffoldingSchemaOptions = {}
  ) {
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
    this.tableNamesToTables = R.pipe(
      // @ts-ignore
      R.unnest,
      R.groupBy(n => n[0]),
      R.map(groupedNameToDef => groupedNameToDef.map(nameToDef => nameToDef[1]))
    )(
      // @ts-ignore
      tableNames.map(tableName => {
        const [schema, table] = this.parseTableName(tableName);
        const tableDefinition = this.resolveTableDefinition(tableName);
        const definition = {
          schema, table, tableDefinition, tableName
        };
        const tableizeName = inflection.tableize(table);
        const parts = tableizeName.split('_');
        const tableNamesFromParts = R.range(0, parts.length - 1).map(toDrop => inflection.tableize(R.drop(toDrop, parts).join('_')));
        const names = R.uniq([table, tableizeName].concat(tableNamesFromParts));
        return names.map(n => [n, definition]);
      })
    );
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
      cube: inflection.camelize(table),
      tableName,
      schema,
      table,
      measures: this.numberMeasures(tableDefinition),
      dimensions,
      drillMembers: this.drillMembers(dimensions),
      joins: includeJoins ? this.joins(tableName, tableDefinition) : []
    };
  }

  protected parseTableName(tableName) {
    let schemaAndTable;
    if (Array.isArray(tableName)) {
      schemaAndTable = tableName;
    } else {
      schemaAndTable = tableName.match(/(["`].*?["`]|[^`".]+)+(?=\s*|\s*$)/g);
    }
    if (schemaAndTable.length !== 2) {
      throw new UserError(`Incorrect format for '${tableName}'. Should be in '<schema>.<table>' format`);
    }
    return schemaAndTable;
  }

  protected dimensions(tableDefinition): Dimension[] {
    return this.dimensionColumns(tableDefinition).map(column => {
      const res: Dimension = {
        name: column.name,
        types: [column.columnType || this.columnType(column)],
        title: inflection.titleize(column.name),
      };

      if (column.columnType !== 'time') {
        res.isPrimaryKey = column.attributes?.includes('primaryKey') ||
          column.name.toLowerCase() === 'id';
      }
      return res;
    });
  }

  protected numberMeasures(tableDefinition) {
    return tableDefinition.filter(
      column => (!column.name.startsWith('_') &&
        (this.columnType(column) === 'number') &&
        (this.options.includeNonDictionaryMeasures ? column.name.toLowerCase() !== 'id' : this.fromMeasureDictionary(column)))
    ).map(column => ({
      name: column.name,
      types: ['sum', 'avg', 'min', 'max'],
      title: inflection.titleize(column.name),
      ...(this.options.includeNonDictionaryMeasures ? { included: this.fromMeasureDictionary(column) } : null)
    }));
  }

  protected fromMeasureDictionary(column) {
    return !column.name.match(new RegExp(idRegex, 'i')) && !!MEASURE_DICTIONARY.find(word => column.name.toLowerCase().endsWith(word));
  }

  protected dimensionColumns(tableDefinition: any) {
    const dimensionColumns = tableDefinition.filter(
      column => !column.name.startsWith('_') && this.columnType(column) === 'string' ||
        column.attributes?.includes('primaryKey') ||
        column.name.toLowerCase() === 'id'
    );

    const timeColumns = R.pipe(
      // @ts-ignore
      R.filter(column => !column.name.startsWith('_') && this.columnType(column) === 'time'),
      R.sortBy(column => this.timeColumnIndex(column)),
      // @ts-ignore
      R.map(column => ({ ...column, columnType: 'time' })) // TODO do we need it?
      // @ts-ignore
    )(tableDefinition);

    return dimensionColumns.concat(timeColumns);
  }

  protected joins(tableName: TableName, tableDefinition) {
    return R.unnest(tableDefinition
      .filter(column => (column.name.match(new RegExp(idRegex, 'i')) && column.name.toLowerCase() !== 'id'))
      .map(column => {
        const withoutId = column.name.replace(new RegExp(idRegex, 'i'), '');
        const tablesToJoin = this.tableNamesToTables[withoutId] ||
          this.tableNamesToTables[inflection.tableize(withoutId)];

        if (!tablesToJoin) {
          return null;
        }

        const columnsToJoin = tablesToJoin.map(definition => {
          if (tableName === definition.tableName) {
            return null;
          }
          let columnForJoin = definition.tableDefinition.find(c => c.name.toLowerCase() === column.name.toLowerCase());
          columnForJoin = columnForJoin || definition.tableDefinition.find(c => c.name.toLowerCase() === 'id');
          if (!columnForJoin) {
            return null;
          }
          return {
            cubeToJoin: inflection.camelize(definition.table),
            columnToJoin: columnForJoin.name,
            tableName: definition.tableName
          };
        }).filter(R.identity);

        return columnsToJoin.map(columnToJoin => ({
          thisTableColumn: column.name,
          tableName: columnToJoin.tableName,
          cubeToJoin: columnToJoin.cubeToJoin,
          columnToJoin: columnToJoin.columnToJoin,
          relationship: 'belongsTo'
        }));
      })
      .filter(R.identity));
  }

  protected drillMembers(dimensions: Dimension[]) {
    return dimensions.filter(d => this.fromDrillMembersDictionary(d));
  }

  protected fromDrillMembersDictionary(dimension) {
    return !!DRILL_MEMBERS_DICTIONARY.find(word => dimension.name.toLowerCase().includes(word));
  }

  protected timeColumnIndex(column): number {
    const name = column.name.toLowerCase();
    if (name.indexOf('create') !== -1) {
      return 0;
    } else if (name.indexOf('update') !== -1) {
      return 1;
    } else {
      return 2;
    }
  }

  protected columnType(column): ColumnType {
    const type = column.type.toLowerCase();
    if (['time', 'date'].find(t => type.indexOf(t) !== -1)) {
      return ColumnType.Time;
    } else if (['int', 'dec', 'double', 'numb'].find(t => type.indexOf(t) !== -1)) {
      // enums are not Numbers
      return ColumnType.Number;
    } else {
      return ColumnType.String;
    }
  }
}
