import inflection from 'inflection';
import { CubeMembers, SchemaContext } from '../ScaffoldingTemplate';
import {
  CubeDescriptor,
  DatabaseSchema,
  Dimension,
  MemberType,
  ScaffoldingSchema,
  TableName,
  TableSchema,
} from '../ScaffoldingSchema';
import { ValueWithComments } from '../descriptors/ValueWithComments';
import { toSnakeCase } from '../utils';

const JOIN_RELATIONSHIP_MAP = {
  hasOne: 'one_to_one',
  has_one: 'one_to_one',
  hasMany: 'one_to_many',
  has_many: 'one_to_many',
  belongsTo: 'many_to_one',
  belongs_to: 'many_to_one',
};

export type SchemaFile = {
  fileName: string;
  content: string;
};

export type SchemaFormatterOptions = {
  snakeCase: boolean;
  catalog?: string | null;
};

export abstract class BaseSchemaFormatter {
  protected readonly scaffoldingSchema: ScaffoldingSchema;

  public constructor(
      protected readonly dbSchema: DatabaseSchema,
      protected readonly driver: any,
      protected readonly options: SchemaFormatterOptions
  ) {
    this.scaffoldingSchema = new ScaffoldingSchema(dbSchema, this.options);
  }

  public abstract fileExtension(): string;

  protected abstract cubeReference(cube: string): string;

  protected abstract renderFile(fileDescriptor: Record<string, unknown>): string;

  public generateFilesByTableNames(
    tableNames: TableName[],
    schemaContext: SchemaContext = {}
  ): SchemaFile[] {
    const tableSchemas = this.scaffoldingSchema.generateForTables(
      tableNames.map((n) => this.scaffoldingSchema.resolveTableName(n))
    );

    return this.generateFilesByTableSchemas(tableSchemas, schemaContext);
  }

  public generateFilesByCubeDescriptors(
    cubeDescriptors: CubeDescriptor[],
    schemaContext: SchemaContext = {}
  ): SchemaFile[] {
    return this.generateFilesByTableSchemas(this.tableSchemasByCubeDescriptors(cubeDescriptors), schemaContext);
  }

  protected generateFilesByTableSchemas(tableSchemas: TableSchema[], schemaContext: SchemaContext = {}): SchemaFile[] {
    const cubeToDimensionNamesMap = new Map(
      tableSchemas.map(tableSchema => [tableSchema.cube, tableSchema.dimensions.map(d => d.name)])
    );

    tableSchemas = tableSchemas.map((tableSchema) => {
      const updatedJoins = tableSchema.joins.map((join) => ({
        ...join,
        thisTableColumnIncludedAsDimension: !!cubeToDimensionNamesMap.get(tableSchema.cube)?.includes(join.thisTableColumn),
        columnToJoinIncludedAsDimension: !!cubeToDimensionNamesMap.get(join.cubeToJoin)?.includes(join.columnToJoin)
      }));

      return {
        ...tableSchema,
        joins: updatedJoins
      };
    });

    return tableSchemas.map((tableSchema) => ({
      fileName: `${tableSchema.cube}.${this.fileExtension()}`,
      content: this.renderFile(this.schemaDescriptorForTable(tableSchema, schemaContext)),
    }));
  }

  protected sqlForMember(m) {
    return `${
      this.escapeName(m.name) !== m.name || !this.eligibleIdentifier(m.name)
        ? `${this.cubeReference('CUBE')}.`
        : ''
    }${this.escapeName(m.name)}`;
  }

  protected memberTitle(m) {
    return inflection.titleize(inflection.underscore(this.memberName(m))) !== m.title
      ? m.title
      : undefined;
  }

  protected memberName(member: { title: string }) {
    const title = member.title.replace(/[^A-Za-z0-9]+/g, '_').toLowerCase();

    if (this.options.snakeCase) {
      return toSnakeCase(title);
    }

    return inflection.camelize(title, true);
  }

  protected escapeName(name) {
    if (this.eligibleIdentifier(name)) {
      return name;
    }
    return this.driver.quoteIdentifier(name);
  }

  protected eligibleIdentifier(name: string) {
    return !!name.match(/^[a-z0-9_]+$/);
  }

  protected schemaDescriptorForTable(tableSchema: TableSchema, schemaContext: SchemaContext = {}) {
    let table = `${
      tableSchema.schema?.length ? `${this.escapeName(tableSchema.schema)}.` : ''
    }${this.escapeName(tableSchema.table)}`;

    if (this.options.catalog) {
      table = `${this.escapeName(this.options.catalog)}.${table}`;
    }

    const { dataSource, ...contextProps } = schemaContext;

    let dataSourceProp = {};
    if (dataSource) {
      dataSourceProp = this.options.snakeCase ? { data_source: dataSource } : { dataSource };
    }

    const sqlOption = this.options.snakeCase
      ? {
        sql_table: table,
      }
      : {
        sql: `SELECT * FROM ${table}`,
      };

    // Try to use dimension refs if possible
    // Source and target columns must be included in the respective cubes as dimensions
    // {CUBE.dimension_name} = {other_cube.other_dimension_name}
    // instead of
    // {CUBE}.dimension_name = {other_cube}.other_dimension_name
    const joins = tableSchema.joins
      .map((j) => {
        const thisTableColumnRef = j.thisTableColumnIncludedAsDimension
          ? this.cubeReference(`CUBE.${this.memberName({ title: j.thisTableColumn })}`)
          : `${this.cubeReference('CUBE')}.${this.escapeName(
            j.thisTableColumn
          )}`;
        const columnToJoinRef = j.columnToJoinIncludedAsDimension
          ? this.cubeReference(`${j.cubeToJoin}.${this.memberName({ title: j.columnToJoin })}`)
          : `${this.cubeReference(j.cubeToJoin)}.${this.escapeName(j.columnToJoin)}`;

        return ({
          [j.cubeToJoin]: {
            sql: `${thisTableColumnRef} = ${columnToJoinRef}`,
            relationship: this.options.snakeCase
              ? (JOIN_RELATIONSHIP_MAP[j.relationship] ?? j.relationship)
              : j.relationship,
          },
        });
      })
      .reduce((a, b) => ({ ...a, ...b }), {});

    return {
      cube: tableSchema.cube,
      ...sqlOption,
      ...dataSourceProp,

      joins,
      dimensions: tableSchema.dimensions.sort((a) => (a.isPrimaryKey ? -1 : 0))
        .map((m) => ({
          [this.memberName(m)]: {
            sql: this.sqlForMember(m),
            type: m.type ?? m.types[0],
            title: this.memberTitle(m),
            [this.options.snakeCase ? 'primary_key' : 'primaryKey']: m.isPrimaryKey
              ? true
              : undefined,
          },
        }))
        .reduce((a, b) => ({ ...a, ...b }), {}),
      measures: tableSchema.measures
        .map((m) => ({
          [this.memberName(m)]: {
            sql: this.sqlForMember(m),
            type: m.type ?? m.types[0],
            title: this.memberTitle(m),
          },
        }))
        .reduce((a, b) => ({ ...a, ...b }), {
          count: {
            type: 'count',
          },
        }),

      ...(this.options.snakeCase
        ? Object.fromEntries(
          Object.entries(contextProps).map(([key, value]) => [toSnakeCase(key), value])
        )
        : contextProps),

      [this.options.snakeCase ? 'pre_aggregations' : 'preAggregations']: new ValueWithComments(
        null,
        [
          'Pre-aggregation definitions go here.',
          'Learn more in the documentation: https://cube.dev/docs/caching/pre-aggregations/getting-started',
        ]
      ),
    };
  }

  protected tableSchemasByCubeDescriptors(cubeDescriptors: CubeDescriptor[]) {
    const tableNames = cubeDescriptors.map(({ tableName }) => tableName);
    const generatedSchemaForTables = this.scaffoldingSchema.generateForTables(
      tableNames.map((n) => this.scaffoldingSchema.resolveTableName(n))
    );

    return cubeDescriptors.map<TableSchema>((descriptor) => {
      const generatedDescriptor = generatedSchemaForTables.find(
        ({ cube }) => cube === descriptor.cube
      );

      const cubeMembers = descriptor.members.reduce<CubeMembers>(
        (memo, member) => ({
          measures: [...memo.measures].concat(
            member.memberType === MemberType.Measure ? [member] : []
          ),
          dimensions: [...memo.dimensions].concat(
            member.memberType === MemberType.Dimension ? [member] : []
          ),
        }),
        {
          measures: [],
          dimensions: [],
        }
      );

      return {
        ...generatedDescriptor,
        ...descriptor,
        ...cubeMembers,
      };
    });
  }
}
