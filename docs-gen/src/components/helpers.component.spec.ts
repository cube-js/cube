import * as fs from 'fs-extra';
import * as Handlebars from 'handlebars';
import * as path from 'path';
import { Application } from 'typedoc';

describe(`HelpersComponent`, () => {
  let app;
  let project;
  let pluginInstance;
  const out = path.join(__dirname, 'tmp');
  beforeAll(() => {
    app = new Application();
    app.bootstrap({
      module: 'CommonJS',
      target: 'ES5',
      readme: 'none',
      theme: 'markdown',
      logger: 'none',
      includes: './test/stubs/inc/',
      media: './test/stubs/media/',
      listInvalidSymbolLinks: true,
      plugin: path.join(__dirname, '../../dist/index'),
    });
    project = app.convert(app.expandInputFiles(['./test/stubs/']));
    app.generateDocs(project, out);
    pluginInstance = app.renderer.getComponent('helpers');
  });

  afterAll(() => {
    fs.removeSync(out);
  });

  test(`should define helper'`, () => {
    const helpers = Handlebars.helpers;
    expect(helpers.comment).toBeDefined();
  });

  test(`should convert symbols brackets to symbol links'`, () => {
    expect(
      Handlebars.helpers.comment.call(project.findReflectionByName('commentsWithSymbolLinks').comment.text),
    ).toMatchSnapshot();
  });

  test(`should set warnings if symbol not found'`, () => {
    expect(pluginInstance.warnings.length > 0).toBeTruthy();
  });

  test(`should convert comments with includes'`, () => {
    expect(
      Handlebars.helpers.comment.call(project.findReflectionByName('commentsWithIncludes').comment.text),
    ).toMatchSnapshot();
  });

  test(`should build @link references'`, () => {
    expect(
      Handlebars.helpers.comment.call(project.findReflectionByName('functionWithDocLink').signatures[0].comment.text),
    ).toMatchSnapshot();
  });
});
