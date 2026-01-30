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

// YAML-specific tests that don't apply to JS
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
});
