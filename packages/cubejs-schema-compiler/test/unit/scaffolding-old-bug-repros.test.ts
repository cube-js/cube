import YAML from 'js-yaml';

import {
  ScaffoldingTemplate,
  SchemaFormat,
} from '../../src/scaffolding/ScaffoldingTemplate';

// Quoting stubs mirror the real drivers:
// - MySqlDriver.quoteIdentifier (packages/cubejs-mysql-driver/src/MySqlDriver.ts)
// - BigQueryDriver.quoteIdentifier (packages/cubejs-bigquery-driver/src/BigQueryDriver.ts)
// - BaseDriver / Postgres double-quote quoting
const mySqlDriver = {
  quoteIdentifier: (identifier: string) => `\`${identifier}\``,
};

const bigQueryDriver = {
  quoteIdentifier(identifier: string) {
    const nestedFields = identifier.split('.');
    return nestedFields
      .map((name) => {
        if (name.match(/^[a-z0-9_]+$/)) {
          return name;
        }
        return `\`${identifier}\``;
      })
      .join('.');
  },
};

const postgresDriver = {
  quoteIdentifier: (identifier: string) => `"${identifier}"`,
};

const dbSchema = {
  ODS: {
    CAMPAIGN_EMAILS: [
      { name: 'ID', type: 'integer', attributes: ['primaryKey'] },
      { name: 'EMAIL', type: 'text', attributes: [] },
      { name: 'SENT_AT', type: 'timestamp', attributes: [] },
    ],
  },
};

type YamlModel = { cubes: Array<{ name: string; sql_table: string; dimensions: Array<{ name: string; sql: string }> }> };

describe('Scaffolding old bug repros', () => {
  // https://github.com/cube-js/cube/issues/6656
  describe('#6656 YAML for uppercase schema/table names', () => {
    it.each([
      ['MySQL (backticks)', mySqlDriver, '`ODS`.`CAMPAIGN_EMAILS`'],
      ['BigQuery (backticks)', bigQueryDriver, '`ODS`.`CAMPAIGN_EMAILS`'],
      ['Postgres (double quotes)', postgresDriver, '"ODS"."CAMPAIGN_EMAILS"'],
    ])('%s: generated YAML parses and sql_table round-trips', (_name, driver, expectedSqlTable) => {
      const template = new ScaffoldingTemplate(dbSchema, driver, {
        format: SchemaFormat.Yaml,
        snakeCase: true,
      });

      const [file] = template.generateFilesByTableNames([['ODS', 'CAMPAIGN_EMAILS']], { dataSource: 'default' });

      let parsed: YamlModel | undefined;
      expect(() => {
        parsed = YAML.load(file.content) as YamlModel;
      }).not.toThrow();

      const cube = parsed!.cubes[0];
      expect(cube.sql_table).toBe(expectedSqlTable);
      expect(cube.dimensions.map((d) => d.name).sort()).toEqual(['email', 'id', 'sent_at']);
    });
  });
});
