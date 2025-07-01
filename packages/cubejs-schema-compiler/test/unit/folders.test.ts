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

  it('folders from view extending other view', async () => {
    const view2 = metaTransformer.cubes.find(
      (it) => it.config.name === 'test_view2'
    );
    const view3 = metaTransformer.cubes.find(
      (it) => it.config.name === 'test_view3'
    );

    expect(view2.config.folders.length).toBe(1);
    expect(view3.config.folders.length).toBe(2);

    const folder1 = view2.config.folders.find(
      (it) => it.name === 'folder1'
    );
    expect(folder1.members).toEqual([
      'test_view2.users_age',
      'test_view2.users_state',
      'test_view2.renamed_orders_status',
    ]);

    const folder1v3 = view3.config.folders.find(
      (it) => it.name === 'folder1'
    );
    expect(folder1v3.members).toEqual([
      'test_view3.users_age',
      'test_view3.users_state',
      'test_view3.renamed_orders_status',
    ]);

    const folder2 = view3.config.folders.find(
      (it) => it.name === 'folder2'
    );
    expect(folder2.members).toEqual(
      expect.arrayContaining(['test_view3.users_city', 'test_view3.users_renamed_in_view3_gender'])
    );
  });

  it('throws errors for folder members with path', async () => {
    const modelContent = fs.readFileSync(
      path.join(process.cwd(), '/test/unit/fixtures/folders_invalid_path.yml'),
      'utf8'
    );
    // eslint-disable-next-line @typescript-eslint/no-shadow
    const { compiler } = prepareYamlCompiler(modelContent);

    try {
      await compiler.compile();
      throw new Error('should throw earlier');
    } catch (e: any) {
      expect(e.toString()).toMatch(/Paths aren't allowed in the 'folders' but 'users.age' has been provided for test_view/);
      expect(e.toString()).toMatch(/Member 'users.age' included in folder 'folder1' not found/);
    }
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
