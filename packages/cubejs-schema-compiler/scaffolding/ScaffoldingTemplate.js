const inflection = require('inflection');
const ScaffoldingSchema = require('./ScaffoldingSchema');
const UserError = require('../compiler/UserError');

class MemberReference {
  constructor(member) {
    this.member = member;
  }
}

class ScaffoldingTemplate {
  constructor(dbSchema, driver) {
    this.dbSchema = dbSchema;
    this.scaffoldingSchema = new ScaffoldingSchema(dbSchema);
    this.driver = driver;
  }

  escapeName(name) {
    if (this.eligibleIdentifier(name)) {
      return name;
    }
    return this.driver.quoteIdentifier(name);
  }

  eligibleIdentifier(name) {
    return !!name.match(/^[a-z0-9_]+$/);
  }

  generateFilesByTableNames(tableNames) {
    const schemaForTables = this.scaffoldingSchema.generateForTables(tableNames.map(n => this.resolveTableName(n)));
    return schemaForTables.map(tableSchema => ({
      // eslint-disable-next-line prefer-template
      fileName: tableSchema.cube + '.js',
      content: this.renderFile(this.schemaDescriptorForTable(tableSchema))
    }));
  }

  // eslint-disable-next-line consistent-return
  resolveTableName(tableName) {
    const tableParts = tableName.split('.');
    if (tableParts.length === 2) {
      this.scaffoldingSchema.resolveTableDefinition(tableName);
      return tableName;
    } else if (tableParts.length === 1) {
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
    } else {
      throw new UserError(`Table names should be in <table> or <schema>.<table> format`);
    }
  }

  schemaDescriptorForTable(tableSchema) {
    return {
      cube: tableSchema.cube,
      sql: `SELECT * FROM ${this.escapeName(tableSchema.schema)}.${this.escapeName(tableSchema.table)}`, // TODO escape
      joins: tableSchema.joins.map(j => ({
        [j.cubeToJoin]: {
          sql: `\${CUBE}.${this.escapeName(j.thisTableColumn)} = \${${j.cubeToJoin}}.${this.escapeName(j.columnToJoin)}`,
          relationship: j.relationship
        }
      })).reduce((a, b) => ({ ...a, ...b }), {}),
      measures: tableSchema.measures.map(m => ({
        [this.memberName(m)]: {
          sql: this.sqlForMember(m),
          type: m.types[0],
          title: this.memberTitle(m)
        }
      })).reduce((a, b) => ({ ...a, ...b }), {
        count: {
          type: 'count',
          drillMembers: tableSchema.drillMembers.map(m => new MemberReference(this.memberName(m)))
        }
      }),
      dimensions: tableSchema.dimensions.map(m => ({
        [this.memberName(m)]: {
          sql: this.sqlForMember(m),
          type: m.types[0],
          title: this.memberTitle(m),
          primaryKey: m.isPrimaryKey ? true : undefined
        }
      })).reduce((a, b) => ({ ...a, ...b }), {})
    };
  }

  sqlForMember(m) {
    // eslint-disable-next-line no-template-curly-in-string
    return `${this.escapeName(m.name) !== m.name || !this.eligibleIdentifier(m.name) ? '${CUBE}.' : ''}${this.escapeName(m.name)}`;
  }

  memberTitle(m) {
    return inflection.titleize(inflection.underscore(this.memberName(m))) !== m.title ? m.title : undefined;
  }

  memberName(member) {
    return inflection.camelize(member.title.replace(/[^A-Za-z0-9]+/g, '_').toLowerCase(), true);
  }

  renderFile(fileDescriptor) {
    const { cube, ...descriptor } = fileDescriptor;
    return `cube(\`${cube}\`, ${this.render(descriptor, 0)});\n`;
  }

  render(descriptor, level) {
    // eslint-disable-next-line prefer-template
    const lineSeparator = ',\n' + (level < 2 ? '\n' : '');
    if (Array.isArray(descriptor)) {
      const items = descriptor.map(desc => this.render(desc, level + 1)).join(', ');
      return `[${items}]`;
    } else if (typeof descriptor === 'string') {
      return `\`${descriptor.replace(/`/g, '\\`')}\``;
    } else if (descriptor instanceof MemberReference) {
      return descriptor.member;
    } else if (typeof descriptor === 'object') {
      let entries = Object.keys(descriptor)
        .filter(k => descriptor[k] != null)
        .map(key => `${key}: ${this.render(descriptor[key], level + 1)}`).join(lineSeparator);
      // eslint-disable-next-line prefer-template
      entries = entries.split('\n').map(l => '  ' + l).join('\n');
      return `{\n${entries}\n}`;
    } else {
      return descriptor.toString();
    }
  }
}

module.exports = ScaffoldingTemplate;
