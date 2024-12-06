import fs from 'fs';
import path from 'path';

import { CubeToMetaTransformer } from 'src/compiler/CubeToMetaTransformer';
import { prepareYamlCompiler } from './PrepareCompiler';

describe('Cube Folders', () => {
  let metaTransformer: CubeToMetaTransformer;
  let compiler;

  beforeAll(async () => {
    const modelContent = fs.readFileSync(
      path.join(process.cwd(), '/test/unit/fixtures/folders.yml'),
      'utf8'
    );
    const prepared = prepareYamlCompiler(modelContent);
    compiler = prepared.compiler;
    metaTransformer = prepared.metaTransformer;

    await compiler.compile();
  });

  it('a folder with includes all and named members', async () => {
    const emptyView = metaTransformer.cubes.find(
      (it) => it.config.name === 'test_view'
    );

    expect(emptyView.config.folders.length).toBe(2);

    const folder1 = emptyView.config.folders.find(
      (it) => it.name === 'folder1'
    );
    expect(folder1.members).toEqual([
      'test_view.age',
      'test_view.renamed_gender',
    ]);

    const folder2 = emptyView.config.folders.find(
      (it) => it.name === 'folder2'
    );
    expect(folder2.members).toEqual(
      expect.arrayContaining(['test_view.age', 'test_view.renamed_gender'])
    );
  });

  it('a folder with aliased and prefixed cubes', async () => {
    const view = metaTransformer.cubes.find(
      (it) => it.config.name === 'test_view2'
    );

    expect(view.config.folders.length).toBe(1);

    const folder1 = view.config.folders.find((it) => it.name === 'folder1');
    expect(folder1.members).toEqual([
      'test_view2.users_age',
      'test_view2.users_state',
      'test_view2.renamed_orders_status',
    ]);
  });
});
