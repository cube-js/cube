import inflection from 'inflection';
import { CubeMembers, SchemaContext } from '../ScaffoldingTemplate';
import { CubeDescriptor, DatabaseSchema, MemberType, ScaffoldingSchema, TableName, TableSchema, } from '../ScaffoldingSchema';
import { MemberReference } from '../descriptors/MemberReference';
import { ValueWithComments } from '../descriptors/ValueWithComments';

export type SchemaFile = {
  fileName: string;
  content: string;
};

export abstract class BaseSchemaFormatter {
  protected readonly scaffoldingSchema: ScaffoldingSchema;
  
  public constructor(
    protected readonly dbSchema: DatabaseSchema,
    protected readonly driver: any,
  ) {
    this.scaffoldingSchema = new ScaffoldingSchema(dbSchema, driver);
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
      content: this.renderFile(
        this.schemaDescriptorForTable(tableSchema, schemaContext)
      ),
    }));
  }
  
  public generateFilesByCubeDescriptors(
    cubeDescriptors: CubeDescriptor[],
    schemaContext: SchemaContext = {}
  ): SchemaFile[] {
    return this.schemaForTablesByCubeDescriptors(cubeDescriptors).map((tableSchema) => ({
      fileName: `${tableSchema.cube}.${this.fileExtension()}`,
      content: this.renderFile(
        this.schemaDescriptorForTable(tableSchema, schemaContext)
      ),
    }));
  }

  protected sqlForMember(m) {
    return `${
      this.escapeName(m.name) !== m.name || !this.eligibleIdentifier(m.name)
        ?
        `${this.cubeReference('CUBE')}.`
        : ''
    }${this.escapeName(m.name)}`;
  }

  protected memberTitle(m) {
    return inflection.titleize(inflection.underscore(this.memberName(m))) !==
      m.title
      ? m.title
      : undefined;
  }

  protected memberName(member) {
    return inflection.camelize(
      member.title.replace(/[^A-Za-z0-9]+/g, '_').toLowerCase(),
      true
    );
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
    return {
      cube: tableSchema.cube,
      sql: `SELECT * FROM ${tableSchema.schema?.length ? `${this.escapeName(tableSchema.schema)}.` : ''}${this.escapeName(tableSchema.table)}`, // TODO escape
      preAggregations: new ValueWithComments(null, [
        'Pre-Aggregations definitions go here',
        'Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started'
      ]),
      joins: tableSchema.joins.map(j => ({
        [j.cubeToJoin]: {
          sql: `${this.cubeReference('CUBE')}.${this.escapeName(j.thisTableColumn)} = ${this.cubeReference(j.cubeToJoin)}.${this.escapeName(j.columnToJoin)}`,
          relationship: j.relationship
        }
      })).reduce((a, b) => ({ ...a, ...b }), {}),
      measures: tableSchema.measures.map(m => ({
        [this.memberName(m)]: {
          sql: this.sqlForMember(m),
          type: m.type ?? m.types[0],
          title: this.memberTitle(m)
        }
      })).reduce((a, b) => ({ ...a, ...b }), {
        count: {
          type: 'count',
          drillMembers: (tableSchema.drillMembers || []).map(m => new MemberReference(this.memberName(m)))
        }
      }),
      dimensions: tableSchema.dimensions.map(m => ({
        [this.memberName(m)]: {
          sql: this.sqlForMember(m),
          type: m.type ?? m.types[0],
          title: this.memberTitle(m),
          primaryKey: m.isPrimaryKey ? true : undefined
        }
      })).reduce((a, b) => ({ ...a, ...b }), {}),
      ...schemaContext
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

      const dimensionNames = cubeMembers.dimensions
        .filter((d) => d.included || d.included == null)
        .map((d) => d.name);

      return {
        ...generatedDescriptor,
        ...descriptor,
        ...cubeMembers,
        drillMembers: generatedDescriptor?.drillMembers?.filter((dm) => dimensionNames.includes(dm.name)),
      };
    });
  }
}
