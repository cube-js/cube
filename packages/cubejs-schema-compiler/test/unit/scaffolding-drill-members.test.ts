import { CubeToMetaTransformer } from '../../src/compiler/CubeToMetaTransformer';
import {
  ScaffoldingTemplate,
  SchemaFormat,
} from '../../src/scaffolding/ScaffoldingTemplate';
import { prepareCompiler } from './PrepareCompiler';

const driver = {
  quoteIdentifier: (name) => `"${name}"`,
};

const dbSchema = {
  public: {
    orders: [
      { name: 'id', type: 'integer', attributes: ['primaryKey'] },
      { name: 'order_status', type: 'character varying', attributes: [] },
      { name: 'amount', type: 'integer', attributes: [] },
      { name: 'created_at', type: 'timestamp', attributes: [] },
    ],
  },
};

/**
 * Generated models are only useful if Cube can actually compile them and resolve the
 * drill members — asserting the emitted text alone would pass on output the compiler
 * rejects, so each format is compiled here and read back out of the meta.
 */
describe('Scaffolding drill members resolve after compilation', () => {
  async function metaFor(format: SchemaFormat, snakeCase: boolean) {
    const [file] = new ScaffoldingTemplate(dbSchema, driver, {
      format,
      snakeCase,
    }).generateFilesByTableNames(['public.orders']);

    const { compiler, metaTransformer } = prepareCompiler({
      content: file.content,
      fileName: file.fileName,
    });

    await compiler.compile();

    return (metaTransformer as CubeToMetaTransformer).cubes[0];
  }

  it.each([
    ['YAML', SchemaFormat.Yaml, true],
    ['JavaScript, snake_case', SchemaFormat.JavaScript, true],
    ['JavaScript, camelCase', SchemaFormat.JavaScript, false],
  ] as const)('compiles the generated %s model with drill members', async (_label, format, snakeCase) => {
    const cube = await metaFor(format, snakeCase);

    const countMeasure = cube.config.measures.find(
      (m) => m.name.endsWith('.count')
    );

    if (!countMeasure) {
      throw new Error('The generated cube has no count measure');
    }

    // The names resolve to real members of the generated cube — an unresolvable
    // reference is what makes a drill-down dead-end at click time.
    const dimensionNames = cube.config.dimensions.map((d) => d.name);
    const { drillMembers } = countMeasure;

    expect(drillMembers.length).toBeGreaterThan(0);
    drillMembers.forEach((member) => {
      expect(dimensionNames).toContain(member);
    });

    // Primary key first, main time dimension last.
    expect(drillMembers[0]).toMatch(/\.id$/);
    expect(drillMembers[drillMembers.length - 1]).toMatch(/\.created_?[aA]t$/);
  });

  it('gives the aggregate measure the same drill members as count', async () => {
    const cube = await metaFor(SchemaFormat.Yaml, true);

    const count = cube.config.measures.find((m) => m.name.endsWith('.count'));
    const amount = cube.config.measures.find((m) => m.name.endsWith('.amount'));

    expect(amount?.drillMembers?.length).toBeGreaterThan(0);
    expect(amount?.drillMembers).toEqual(count?.drillMembers);
  });
});
