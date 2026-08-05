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
import { MemberReference } from '../descriptors/MemberReference';
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

  /**
   * The members that identify one source row: every dimension the cube defines, with the
   * primary key first and the time dimensions last.
   *
   * Every dimension, deliberately. Which columns are meaningful to drill into is not
   * recoverable from a warehouse schema — the generator would have to guess from column
   * names, and a name is not evidence of meaning. So the set isn't narrowed at all: a
   * member the user doesn't want is one they delete from generated output they were
   * going to review anyway, whereas one that was never emitted is one they must know to
   * add. Ordering is the only editorial judgement here, and it costs nothing to be wrong
   * about — key first because it identifies the row, time last because it reads as
   * "when".
   *
   * Derived from the dimensions actually being rendered rather than from the ones
   * ScaffoldingSchema computed: the cube-descriptor path lets the caller drop members,
   * and a drill member the cube doesn't define dead-ends at click time.
   */
  protected drillMembers(dimensions: Dimension[]): Dimension[] {
    const isTime = (d: Dimension) => (d.type ?? d.types?.[0]) === 'time';

    // Deduped because dimensions render into an object keyed by member name: two columns
    // that collapse to one name must not repeat in the drill list.
    const candidates = this.dedupeByMemberName(dimensions);

    const primaryKeys = candidates.filter((d) => d.isPrimaryKey);
    const attributes = candidates.filter((d) => !d.isPrimaryKey && !isTime(d));
    // All of them, in the order they render. Picking one "main" timestamp would mean
    // ranking `created_at` above `deleted_at` by what the names suggest.
    const timeDimensions = candidates.filter((d) => !d.isPrimaryKey && isTime(d));

    return [...primaryKeys, ...attributes, ...timeDimensions];
  }

  /**
   * Dimensions are rendered into an object keyed by member name, so columns that
   * collapse to the same name yield one dimension — the drill list must collapse too,
   * or the drill-down repeats a column.
   *
   * Last-wins, matching the spread-reduce that renders `dimensions`: on a collision the
   * cube keeps the later definition, so classifying off the earlier one would read
   * `isPrimaryKey` / `isTime` from a definition the cube discarded.
   */
  private dedupeByMemberName(dimensions: Dimension[]): Dimension[] {
    return [
      ...dimensions
        .reduce(
          (memo, d) => memo.set(this.memberName(d), d),
          new Map<string, Dimension>()
        )
        .values(),
    ];
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

    const sortedDimensions = tableSchema.dimensions.sort((a) => (a.isPrimaryKey ? -1 : 0));

    const drillMembers = this.drillMembers(sortedDimensions);
    const drillMembersProp = drillMembers.length
      ? {
        [this.options.snakeCase ? 'drill_members' : 'drillMembers']: drillMembers.map(
          (m) => new MemberReference(this.memberName(m))
        ),
      }
      : {};

    return {
      cube: tableSchema.cube,
      ...sqlOption,
      ...dataSourceProp,

      joins,
      dimensions: sortedDimensions
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
            ...drillMembersProp,
          },
        }))
        .reduce((a, b) => ({ ...a, ...b }), {
          count: {
            type: 'count',
            ...drillMembersProp,
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
