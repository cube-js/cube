const inflection = require('inflection');
const UserError = require('../compiler/UserError');
const R = require('ramda');

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

class ScaffoldingSchema {
  constructor(dbSchema) {
    this.dbSchema = dbSchema;
  }

  generateForTables(tableNames) {
    this.prepareTableNamesToTables(tableNames);
    return tableNames.map(tableName => this.tableSchema(tableName, true));
  }

  prepareTableNamesToTables(tableNames) {
    this.tableNamesToTables = R.pipe(
      R.unnest,
      R.groupBy(n => n[0]),
      R.map(groupedNameToDef => groupedNameToDef.map(nameToDef => nameToDef[1]))
    )(
      tableNames.map(tableName => {
        const [schema, table] = this.parseTableName(tableName);
        const tableDefinition = this.resolveTableDefinition(tableName);
        const definition = { schema, table, tableDefinition, tableName };
        const tableizeName = inflection.tableize(table);
        const parts = tableizeName.split('_');
        const tableNamesFromParts = R.range(0, parts.length - 1).map(toDrop => inflection.tableize(R.drop(toDrop, parts).join('_')));
        const names = R.uniq([table, tableizeName].concat(tableNamesFromParts));
        return names.map(n => [n, definition]);
      })
    );
  }

  resolveTableDefinition(tableName) {
    const [schema, table] = this.parseTableName(tableName);
    if (!this.dbSchema[schema]) {
      throw new UserError(`Can't resolve ${tableName}: '${schema}' does not exist`);
    }
    if (!this.dbSchema[schema][table]) {
      throw new UserError(`Can't resolve ${tableName}: '${table}' does not exist`);
    } 
    return this.dbSchema[schema][table];
  }

  tableSchema(tableName, includeJoins) {
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
    }
  }

  parseTableName(tableName) {
    const schemaAndTable = tableName.split('.');
    if (schemaAndTable.length !== 2) {
      throw new UserError(`Incorrect format for '${tableName}'. Should be in '<schema>.<table>' format`);
    }
    return schemaAndTable;
  }

  dimensions(tableDefinition) {
    return this.dimensionColumns(tableDefinition).map(column => {
      const res = {
        name: column.name,
        types: [column.columnType || this.columnType(column)],
        title: inflection.titleize(column.name)
      };

      if (column.columnType !== 'time') {
        res.isPrimaryKey = column.attributes && column.attributes.indexOf('primaryKey') !== -1 ||
          column.name.toLowerCase() === 'id';
      }
      return res;
    })
  }

  numberMeasures(tableDefinition) {
    return tableDefinition.filter(column =>
      !column.name.startsWith('_') && 
      (this.columnType(column) === 'number') &&
      this.fromMeasureDictionary(column)
    ).map(column => ({
      name: column.name,
      types: ['sum', 'avg', 'min', 'max'],
      title: inflection.titleize(column.name)
    }));
  }

  fromMeasureDictionary(column) {
    return !column.name.match(new RegExp(idRegex, "i")) && !!MEASURE_DICTIONARY.find(word => column.name.toLowerCase().endsWith(word));
  }

  dimensionColumns(tableDefinition) {
    const dimensionColumns = tableDefinition.filter(
      column =>
        !column.name.startsWith('_') && this.columnType(column) === 'string' ||
        column.attributes && column.attributes.primaryKey ||
        column.name.toLowerCase() === 'id'
    );

    const timeColumns = R.pipe(
      R.filter(column => !column.name.startsWith('_') && this.columnType(column) === 'time'),
      R.sortBy(column => this.timeColumnIndex(column)),
      R.map(column => ({ ...column, columnType: 'time' })) //TODO do we need it?
    )(tableDefinition);

    return dimensionColumns.concat(timeColumns);
  };

  joins(tableName, tableDefinition) {
    return R.unnest(tableDefinition
      .filter(column => (column.name.match(new RegExp(idRegex, "i")) && column.name.toLowerCase() !== 'id'))
      .map(column => {
        const withoutId = column.name.replace(new RegExp(idRegex, "i"), '');
        const tablesToJoin = this.tableNamesToTables[withoutId] || this.tableNamesToTables[inflection.tableize(withoutId)];

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
          }
        }).filter(R.identity);

        return columnsToJoin.map(columnToJoin => ({
          thisTableColumn: column.name,
          tableName: columnToJoin.tableName,
          cubeToJoin: columnToJoin.cubeToJoin,
          columnToJoin: columnToJoin.columnToJoin,
          relationship: 'belongsTo'
        }))
      })
      .filter(R.identity));
  }

  drillMembers(dimensions) {
    return dimensions.filter(d => this.fromDrillMembersDictionary(d));
  }

  fromDrillMembersDictionary(dimension) {
    return !!DRILL_MEMBERS_DICTIONARY.find(word => dimension.name.toLowerCase().indexOf(word) !== -1)
  }

  timeColumnIndex(column) {
    const name = column.name.toLowerCase();
    if (name.indexOf('create') !== -1) {
      return 0;
    } else if (name.indexOf('update') !== -1) {
      return 1;
    } else {
      return 2;
    }
  }

  columnType(column) {
    const type = column.type.toLowerCase();
    if (['time', 'date'].find(t => type.indexOf(t) !== -1)) {
      return 'time'
    } else if (['int', 'dec', 'double', 'num'].find(t => type.indexOf(t) !== -1)) {
      return 'number'
    } else {
      return 'string';
    }
  }
}

module.exports = ScaffoldingSchema;