import * as path from 'path';
import { Application } from 'typedoc';

const { FrontMatterComponent } = require('components/front-matter.component');
describe(`FrontMatterComponent`, () => {
  let frontMatterComponent;
  let app;

  beforeAll(() => {
    app = new Application();
    app.bootstrap({
      mode: 'file',
      module: 'CommonJS',
      target: 'ES5',
      readme: 'none',
      theme: 'markdown',
      logger: 'none',
      plugin: path.join(__dirname, '../../../dist/index'),
    });
    app.convert(['./test/stubs/functions.ts']);
    app.renderer.addComponent('frontmatter', new FrontMatterComponent(app.renderer));
    frontMatterComponent = app.renderer.getComponent('frontmatter');
  });

  test(`should prepend YAML block to start of page`, () => {
    expect(true).toBeTruthy();
  });

  test(`should prepend YAML block to start of page`, () => {
    const spy = jest.spyOn(frontMatterComponent, 'getTitleFromNavigation').mockReturnValue('Page title');
    const page = {
      contents: '[CONTENT]',
      url: 'modules/_access_access_.md',
      model: { name: '"access/access"' },
      project: { url: 'index.md' },
    };
    frontMatterComponent.onPageEnd(page);
    expect(page.contents).toMatchSnapshot();
    spy.mockRestore();
  });

  test(`should set id`, () => {
    const page = { url: 'modules/_access_access_.md' };
    expect(frontMatterComponent.getId(page)).toMatchSnapshot();
  });

  test(`should set correct title for index page`, () => {
    const page = {
      url: 'index.md',
      project: { url: 'index.md', name: 'Project name' },
    };
    expect(frontMatterComponent.getTitle(page)).toMatchSnapshot();
  });

  test(`should set correct title for pages without a navigation match`, () => {
    const spy = jest.spyOn(frontMatterComponent, 'getTitleFromNavigation').mockReturnValue(null);
    const page = {
      url: 'index.md',
      project: { url: 'page.md', name: 'Project name' },
    };
    expect(frontMatterComponent.getTitle(page)).toMatchSnapshot();
    spy.mockRestore();
  });

  test(`should set correct title for index page if packageInfo label available`, () => {
    const page = {
      url: 'index.md',
      project: { url: 'index.md', packageInfo: { label: 'Package Label' } },
    };
    expect(frontMatterComponent.getTitle(page)).toMatchSnapshot();
  });

  test(`should compile set correct label for index without a README`, () => {
    const spy = jest.spyOn(frontMatterComponent, 'getTitleFromNavigation').mockReturnValue(null);
    const page = {
      url: 'index.md',
      project: { url: 'index.md' },
    };
    expect(frontMatterComponent.getLabel(page)).toMatchSnapshot();
    spy.mockRestore();
  });

  test(`should set correct label for index with a README`, () => {
    const spy = jest.spyOn(frontMatterComponent, 'getTitleFromNavigation').mockReturnValue(null);
    const page = {
      url: 'index.md',
      project: { url: 'index.md', readme: 'README' },
    };
    expect(frontMatterComponent.getLabel(page)).toMatchSnapshot();
    spy.mockRestore();
  });

  test(`should  set correct label for globals file`, () => {
    const page = {
      url: 'globals.md',
      project: { url: 'index.md' },
    };
    expect(frontMatterComponent.getLabel(page)).toMatchSnapshot();
  });

  test(`should parse a quoted string`, () => {
    expect(frontMatterComponent.escapeYAMLString(`xyx's "quoted" title`)).toMatchSnapshot();
  });

  test(`should find title from navigation object`, () => {
    const page = {
      navigation: {
        children: [
          {
            url: 'urla',
            title: 'titlea',
            children: [
              { url: 'urla1', title: 'titlea1' },
              {
                url: 'urlb2',
                title: 'titleb2',
                children: [{ url: 'urlc1', title: 'titlec1' }],
              },
            ],
          },
          { url: 'urlb', title: 'titleb' },
        ],
      },
    };
    expect(frontMatterComponent.getTitleFromNavigation(page, 'urlb')).toEqual('titleb');
    expect(frontMatterComponent.getTitleFromNavigation(page, 'urla1')).toEqual('titlea1');
    expect(frontMatterComponent.getTitleFromNavigation(page, 'urlc1')).toEqual('titlec1');
    expect(frontMatterComponent.getTitleFromNavigation(page, 'url')).toEqual(null);
  });
});
