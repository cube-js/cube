import inflection from 'inflection';
import { CubeMembers, SchemaContext } from '../ScaffoldingTemplate';
import {
  CubeDescriptor,
  DatabaseSchema,
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
    const schemaForTables = this.scaffoldingSchema.generateForTables(
      tableNames.map((n) => this.scaffoldingSchema.resolveTableName(n))
    );

    return schemaForTables.map((tableSchema) => ({
      fileName: `${tableSchema.cube}.${this.fileExtension()}`,
      content: this.renderFile(this.schemaDescriptorForTable(tableSchema, schemaContext)),
    }));
  }

  public generateFilesByCubeDescriptors(
    cubeDescriptors: CubeDescriptor[],
    schemaContext: SchemaContext = {}
  ): SchemaFile[] {
    return this.schemaForTablesByCubeDescriptors(cubeDescriptors).map((tableSchema) => ({
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

  protected memberName(member) {
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

  public schemaDescriptorForTable(tableSchema: TableSchema, schemaContext: SchemaContext = {}) {
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

    return {
      cube: tableSchema.cube,
      ...sqlOption,
      ...dataSourceProp,

      joins: tableSchema.joins
        .map((j) => ({
          [j.cubeToJoin]: {
            sql: `${this.cubeReference('CUBE')}.${this.escapeName(
              j.thisTableColumn
            )} = ${this.cubeReference(j.cubeToJoin)}.${this.escapeName(j.columnToJoin)}`,
            relationship: this.options.snakeCase
              ? (JOIN_RELATIONSHIP_MAP[j.relationship] ?? j.relationship)
              : j.relationship,
          },
        }))
        .reduce((a, b) => ({ ...a, ...b }), {}),
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

  protected schemaForTablesByCubeDescriptors(cubeDescriptors: CubeDescriptor[]) {
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
