import { prepareYamlCompiler } from './PrepareCompiler';

describe('Yaml Schema Testing', () => {
  it('members must be defined as arrays', async () => {
    const { compiler } = prepareYamlCompiler(
      `
      cubes:
      - name: Products
        sql: "select * from tbl"
        dimensions:
          name: Title
          sql: name
          type: string
      `
    );

    try {
      await compiler.compile();

      throw new Error('compile must return an error');
    } catch (e: any) {
      expect(e.message).toContain('dimensions must be defined as array');
    }
  });

  it('commented file crash', async () => {
    const { compiler } = prepareYamlCompiler(
      `
      #cubes:
      #- name: Products
      #  sql: "select * from tbl"
      #  dimensions:
      #    name: Title
      #    sql: name
      #    type: string
      `
    );

    await compiler.compile();
  });
});
