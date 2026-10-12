import YAML from 'js-yaml';
import { ScaffoldingTemplate, SchemaFormat } from '../../src/scaffolding/ScaffoldingTemplate';
import { prepareCompiler } from './PrepareCompiler';

// https://github.com/cube-js/cube/issues/6656
// Uppercase schema/table names are quoted with backticks (BigQuery/MySQL), and the
// YAML formatter emits them as a plain scalar, which is invalid YAML.
const bigQueryDriver = {
  quoteIdentifier: (name: string) => `\`${name}\``,
};

describe('issue #6656: YAML scaffolding with uppercase schema/table names', () => {
  const dbSchema = {
    ODS: {
      CAMPAIGN_EMAILS: [
        { name: 'id', type: 'integer', attributes: [] },
        { name: 'amount', type: 'integer', attributes: [] },
      ],
    },
  };

  it('generates parseable YAML whose sql_table keeps the quoted identifiers', async () => {
    const template = new ScaffoldingTemplate(dbSchema, bigQueryDriver, {
      format: SchemaFormat.Yaml,
      snakeCase: true,
    });

    const [file] = template.generateFilesByTableNames(['ODS.CAMPAIGN_EMAILS']);

    const parsed: any = YAML.load(file.content);
    expect(parsed.cubes[0].sql_table).toEqual('`ODS`.`CAMPAIGN_EMAILS`');

    const { compiler } = prepareCompiler(file);
    await compiler.compile();
  });
});
