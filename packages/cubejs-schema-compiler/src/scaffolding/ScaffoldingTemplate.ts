import inflection from 'inflection';
import { CubeDescriptor, CubeDescriptorMember, DatabaseSchema, MemberType, ScaffoldingSchema, TableName, TableSchema } from './ScaffoldingSchema';
import { UserError } from '../compiler';
import { ValueWithComments } from './ValueWithComments';

type SchemaContext = {
  dataSource?: string;
};

type CubeMembers = {
  measures: CubeDescriptorMember[];
  dimensions: CubeDescriptorMember[];
};

class MemberReference {
  public constructor(public member) {
    this.member = member;
  }
}

export class ScaffoldingTemplate {
  private readonly scaffoldingSchema: ScaffoldingSchema;

  public constructor(private readonly dbSchema: DatabaseSchema, private readonly driver) {
    this.scaffoldingSchema = new ScaffoldingSchema(dbSchema);
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

  public generateFilesByTableNames(tableNames: TableName[], schemaContext: SchemaContext = {}) {
    const schemaForTables = this.scaffoldingSchema.generateForTables(tableNames.map(n => this.resolveTableName(n)));

    return schemaForTables.map(tableSchema => ({
      // eslint-disable-next-line prefer-template
      fileName: tableSchema.cube + '.js',
      content: this.renderFile(this.schemaDescriptorForTable(tableSchema, schemaContext))
    }));
  }

  public generateFilesByCubeDescriptors(cubeDescriptors: CubeDescriptor[], schemaContext: SchemaContext = {}) {
    const tableNames = cubeDescriptors.map(({ tableName }) => tableName);
    const generatedSchemaForTables = this.scaffoldingSchema.generateForTables(tableNames.map(n => this.resolveTableName(n)));

    const schemaForTables = cubeDescriptors.map<TableSchema>((descriptor) => {
      const generatedDescriptor = generatedSchemaForTables.find(({ cube }) => cube === descriptor.cube);
      
      const cubeMembers = descriptor.members.reduce<CubeMembers>((memo, member) => ({
        measures: [...memo.measures].concat(member.memberType === MemberType.Measure ? [member] : []),
        dimensions: [...memo.dimensions].concat(member.memberType === MemberType.Dimension ? [member] : []),
      }), {
        measures: [],
        dimensions: []
      });
      
      const dimensionNames = cubeMembers.dimensions.filter((d) => d.included || d.included == null).map((d) => d.name);

      return {
        ...generatedDescriptor,
        ...descriptor,
        ...cubeMembers,
        drillMembers: generatedDescriptor?.drillMembers?.filter((dm) => dimensionNames.includes(dm.name))
      };
    });
    
    return schemaForTables.map(tableSchema => ({
      fileName: `${tableSchema.cube}.js`,
      content: this.renderFile(this.schemaDescriptorForTable(tableSchema, schemaContext))
    }));
  }

  // eslint-disable-next-line consistent-return
  protected resolveTableName(tableName: TableName) {
    let tableParts;
    if (Array.isArray(tableName)) {
      tableParts = tableName;
    } else {
      tableParts = tableName.match(/(["`].*?["`]|[^`".]+)+(?=\s*|\s*$)/g);
    }

    if (tableParts.length === 2) {
      this.scaffoldingSchema.resolveTableDefinition(tableName);
      return tableName;
    } else if (tableParts.length === 1 && typeof tableName === 'string') {
      const schema = Object.keys(this.dbSchema).find(
        tableSchema => this.dbSchema[tableSchema][tableName] ||
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

    throw new UserError('Table names should be in <table> or <schema>.<table> format');
  }

  public schemaDescriptorForTable(tableSchema: TableSchema, schemaContext: SchemaContext = {}) {
    return {
      cube: tableSchema.cube,
      sql: `SELECT * FROM ${tableSchema.schema && tableSchema.schema.length ? `${this.escapeName(tableSchema.schema)}.` : ''}${this.escapeName(tableSchema.table)}`, // TODO escape
      preAggregations: new ValueWithComments({}, [
        'Pre-Aggregations definitions go here',
        'Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started'
      ]),
      joins: tableSchema.joins.map(j => ({
        [j.cubeToJoin]: {
          sql: `\${CUBE}.${this.escapeName(j.thisTableColumn)} = \${${j.cubeToJoin}}.${this.escapeName(j.columnToJoin)}`,
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

  protected sqlForMember(m) {
    // eslint-disable-next-line no-template-curly-in-string
    return `${this.escapeName(m.name) !== m.name || !this.eligibleIdentifier(m.name) ? '${CUBE}.' : ''}${this.escapeName(m.name)}`;
  }

  protected memberTitle(m) {
    return inflection.titleize(inflection.underscore(this.memberName(m))) !== m.title ? m.title : undefined;
  }

  protected memberName(member) {
    return inflection.camelize(member.title.replace(/[^A-Za-z0-9]+/g, '_').toLowerCase(), true);
  }

  protected renderFile(fileDescriptor) {
    const { cube, ...descriptor } = fileDescriptor;
    return `cube(\`${cube}\`, ${this.render(descriptor, 0)});\n`;
  }

  protected render(descriptor, level, appendComment = '') {
    // eslint-disable-next-line prefer-template
    const lineSeparator = ',\n' + (level < 2 ? '\n' : '');
    if (Array.isArray(descriptor)) {
      const items = descriptor.map(desc => this.render(desc, level + 1)).join(', ');
      return `[${items}]`;
    } else if (typeof descriptor === 'string') {
      return `\`${descriptor.replace(/`/g, '\\`')}\``;
    } else if (descriptor instanceof MemberReference) {
      return descriptor.member;
    } else if (descriptor instanceof ValueWithComments) {
      return this.render(
        descriptor.value,
        level,
        descriptor.comments.map((comment) => `  // ${comment}`).join('\n')
      );
    } else if (typeof descriptor === 'object') {
      const content = Object.keys(descriptor)
        .filter(k => descriptor[k] != null)
        .map(key => `${key}: ${this.render(descriptor[key], level + 1)}`)
        .join(lineSeparator)
        .split('\n')
        .map(l => `  ${l}`)
        .join('\n');

      return `{\n${appendComment}${content}\n}`;
    } else {
      return descriptor.toString();
    }
  }
}
