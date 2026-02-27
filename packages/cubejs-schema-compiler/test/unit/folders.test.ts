import fs from 'fs';
import path from 'path';

import { prepareCompiler, prepareYamlCompiler } from './PrepareCompiler';
import { runFoldersTestSuite } from './folders.abstract';

runFoldersTestSuite(
  'Cube Folders (YAML)',
  () => {
    const modelContent = fs.readFileSync(
      path.join(process.cwd(), '/test/unit/fixtures/folders.yml'),
      'utf8'
    );
    return prepareYamlCompiler(modelContent);
  }
);

runFoldersTestSuite(
  'Cube Folders (JS)',
  () => {
    const modelContent = fs.readFileSync(
      path.join(process.cwd(), '/test/unit/fixtures/folders.js'),
      'utf8'
    );
    return prepareCompiler({
      content: modelContent,
      fileName: 'folders.js',
    });
  }
);

describe('Cube Folders (YAML-specific)', () => {
  it('throws errors for folder members with path', async () => {
    const modelContent = fs.readFileSync(
      path.join(process.cwd(), '/test/unit/fixtures/folders_invalid_path.yml'),
      'utf8'
    );
    const { compiler } = prepareYamlCompiler(modelContent);

    try {
      await compiler.compile();
      throw new Error('should throw earlier');
    } catch (e: any) {
      expect(e.toString()).toMatch(/Paths aren't allowed in the 'folders' but 'users.age' has been provided for test_view/);
      expect(e.toString()).toMatch(/Paths aren't allowed in the 'folders' but 'users.renamed_gender' has been provided for test_view/);
      expect(e.toString()).toMatch(/Member 'users.age' included in folder 'folder1' not found/);
    }
  });

  it('throws error for non-existent cube in folder join_path', async () => {
    const modelContent = `
cubes:
  - name: orders
    sql: SELECT * FROM orders
    measures:
      - name: count
        sql: id
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string

views:
  - name: test_view
    cubes:
      - join_path: orders
        prefix: true
        includes: "*"
    folders:
      - name: folder1
        includes:
          - join_path: nonexistent_cube
`;
    const { compiler } = prepareYamlCompiler(modelContent);

    try {
      await compiler.compile();
      throw new Error('should throw earlier');
    } catch (e: any) {
      expect(e.toString()).toMatch(/nonexistent_cube is not defined/);
    }
  });
});
