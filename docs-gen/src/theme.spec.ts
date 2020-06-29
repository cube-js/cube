import * as fs from 'fs-extra';
import * as path from 'path';
import { Application, UrlMapping } from 'typedoc';

describe(`MarkdownTheme`, () => {
  function getExpectedUrls(urlMappings: UrlMapping[]) {
    const expectedUrls = [];
    urlMappings.forEach(urlMapping => {
      expectedUrls.push(urlMapping.url);
      urlMapping.model.children.forEach(reflection => {
        expectedUrls.push(reflection.url);
      });
    });
    return expectedUrls;
  }

  let app;
  let project;
  let theme;
  const out = path.join(__dirname, 'tmp');
  beforeAll(() => {
    app = new Application();
    app.bootstrap({
      module: 'CommonJS',
      target: 'ES5',
      readme: 'none',
      theme: 'markdown',
      logger: 'none',
      plugin: path.join(__dirname, '../dist/index'),
    });
    project = app.convert(app.expandInputFiles(['./test/stubs/']));
    app.generateDocs(project, out);
    theme = app.renderer.theme;
  });

  afterAll(() => {
    fs.removeSync(out);
  });

  describe(`getUrls`, () => {
    test(`should getUrls'`, () => {
      const urlMappings = theme.getUrls(project);
      expect(getExpectedUrls(urlMappings)).toMatchSnapshot();
    });

    test(`should getUrls when readme is defined`, () => {
      const spy = jest.spyOn(app.options, 'getValue').mockReturnValue('./README.md');
      const urlMappings = theme.getUrls(project);
      expect(getExpectedUrls(urlMappings)).toMatchSnapshot();
      spy.mockRestore();
    });

    test(`should get navigation`, () => {
      expect(theme.getNavigation(project)).toMatchSnapshot();
    });
  });

  describe(`output directory`, () => {
    let directoryListingSpy;

    beforeAll(() => {
      directoryListingSpy = jest.spyOn(fs, 'readdirSync');
    });

    test(`should test output directory true with all allowed files and directories`, () => {
      directoryListingSpy.mockReturnValue([
        '.DS_Store',
        'README.md',
        'globals.md',
        'classes',
        'enums',
        'interfaces',
        'media',
        'modules',
      ]);
      expect(theme.isOutputDirectory('/path')).toBeTruthy();
    });

    test(`should test output directory true with some files directories`, () => {
      directoryListingSpy.mockReturnValue(['README.md', 'classes', 'media', 'modules']);
      expect(theme.isOutputDirectory('/path')).toBeTruthy();
    });

    test(`should test output directory true with just index`, () => {
      directoryListingSpy.mockReturnValue(['README.md']);
      expect(theme.isOutputDirectory('/path')).toBeTruthy();
    });

    test(`should test output directory false with unkown index`, () => {
      directoryListingSpy.mockReturnValue(['Unrecognised.md', 'classes', 'enums', 'interfaces', 'media', 'modules']);
      expect(theme.isOutputDirectory('/path')).toBeFalsy();
    });

    test(`should test output directory false with hidden files`, () => {
      directoryListingSpy.mockReturnValue(['.git', 'classes', 'enums', 'interfaces', 'media', 'modules']);
      expect(theme.isOutputDirectory('/path')).toBeFalsy();
    });

    test(`should test output directory false without an index`, () => {
      directoryListingSpy.mockReturnValue(['globals.md', 'classes', 'enums', 'interfaces', 'media', 'modules']);
      expect(theme.isOutputDirectory('/path')).toBeFalsy();
    });

    test(`should test output directory false with unknown folder`, () => {
      directoryListingSpy.mockReturnValue(['README.md', 'folder']);
      expect(theme.isOutputDirectory('/path')).toBeFalsy();
    });
  });

  describe(`output directory`, () => {
    let directoryListingSpy;
    beforeAll(() => {
      directoryListingSpy = jest.spyOn(fs, 'readdirSync');
    });

    test(`should test output directory true with all allowed files and directories`, () => {
      directoryListingSpy.mockReturnValue([
        '.DS_Store',
        'README.md',
        'globals.md',
        'classes',
        'enums',
        'interfaces',
        'media',
        'modules',
      ]);
      expect(theme.isOutputDirectory('/path')).toBeTruthy();
    });

    test(`should test output directory true with some files directories`, () => {
      directoryListingSpy.mockReturnValue(['README.md', 'classes', 'media', 'modules']);
      expect(theme.isOutputDirectory('/path')).toBeTruthy();
    });

    test(`should test output directory true with just index`, () => {
      directoryListingSpy.mockReturnValue(['README.md']);
      expect(theme.isOutputDirectory('/path')).toBeTruthy();
    });

    test(`should test output directory false with unkown index`, () => {
      directoryListingSpy.mockReturnValue(['Unrecognised.md', 'classes', 'enums', 'interfaces', 'media', 'modules']);
      expect(theme.isOutputDirectory('/path')).toBeFalsy();
    });

    test(`should test output directory false with hidden files`, () => {
      directoryListingSpy.mockReturnValue(['.git', 'classes', 'enums', 'interfaces', 'media', 'modules']);
      expect(theme.isOutputDirectory('/path')).toBeFalsy();
    });

    test(`should test output directory false without an index`, () => {
      directoryListingSpy.mockReturnValue(['globals.md', 'classes', 'enums', 'interfaces', 'media', 'modules']);
      expect(theme.isOutputDirectory('/path')).toBeFalsy();
    });

    test(`should test output directory false with unknown folder`, () => {
      directoryListingSpy.mockReturnValue(['README.md', 'folder']);
      expect(theme.isOutputDirectory('/path')).toBeFalsy();
    });
  });
});
