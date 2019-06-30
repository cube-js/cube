const inflection = require('inflection');
const ScaffoldingSchema = require('./ScaffoldingSchema');
const UserError = require('../compiler/UserError');

class ScaffoldingTemplate {
  constructor(dbSchema, driver) {
    this.dbSchema = dbSchema;
    this.scaffoldingSchema = new ScaffoldingSchema(dbSchema);
    this.driver = driver;
  }

  escapeName(name) {
    if (name.match(/^[a-z0-9_]+$/)) {
      return name;
    }
    return this.driver.quoteIdentifier(name);
  }

  generateFilesByTableNames(tableNames) {
    const schemaForTables = this.scaffoldingSchema.generateForTables(tableNames.map(n => this.resolveTableName(n)));
    return schemaForTables.map(tableSchema => ({
      fileName: tableSchema.cube + '.js',
      content: this.renderFile(this.schemaDescriptorForTable(tableSchema))
    }));
  }

  resolveTableName(tableName) {
    const tableParts = tableName.split('.');
    if (tableParts.length === 2) {
      this.scaffoldingSchema.resolveTableDefinition(tableName);
      return tableName;
    } else if (tableParts.length === 1) {
      const schema = Object.keys(this.dbSchema).find(schema => this.dbSchema[schema][tableName] || this.dbSchema[schema][inflection.tableize(tableName)]);
      if (!schema) {
        throw new UserError(`Can't find any table with '${tableName}' name`);
      }
      if (this.dbSchema[schema][tableName]){
        return `${schema}.${tableName}`;
      }
      if (this.dbSchema[schema][inflection.tableize(tableName)]){
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
          sql: `${this.escapeName(m.name) !== m.name ? '${CUBE}.' : ''}${this.escapeName(m.name)}`,
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
          sql: `${this.escapeName(m.name) !== m.name ? '${CUBE}.' : ''}${this.escapeName(m.name)}`,
          type: m.types[0],
          title: this.memberTitle(m),
          primaryKey: m.isPrimaryKey ? true : undefined
        }
      })).reduce((a, b) => ({ ...a, ...b }), {})
    }
  }

  memberTitle(m) {
    return inflection.titleize(inflection.underscore(this.memberName(m))) !== m.title ? m.title : undefined;
  }

  memberName(member) {
    return inflection.camelize(member.title.replace(/\s+|\./g, '_').toLowerCase(), true);
  }

  renderFile(fileDescriptor) {
    const { cube, ...descriptor } = fileDescriptor;
    return `cube(\`${cube}\`, ${this.render(descriptor, 0)});\n`;
  }

  render(descriptor, level) {
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
      entries = entries.split('\n').map(l => '  ' + l).join('\n');
      return `{\n${entries}\n}`
    } else {
      return descriptor.toString();
    }
  }
}

class MemberReference {
  constructor(member) {
    this.member = member;
  }
}

module.exports = ScaffoldingTemplate;