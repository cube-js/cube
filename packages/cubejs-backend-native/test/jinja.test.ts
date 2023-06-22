import fs from 'fs';
import path from 'path';

import * as native from '../js';

const suite = native.isFallbackBuild() ? xdescribe : describe;

function loadTemplateFile(fileName: string): void {
  const content = fs.readFileSync(path.join(process.cwd(), 'test', 'templates', fileName), 'utf8');

  native.loadTemplate(fileName, content);
}

function testTemplateBySnapshot(templateName: string, ctx: unknown) {
  test(`render ${templateName}`, async () => {
    const actual = native.renderTemplate(templateName, ctx);

    expect(actual).toMatchSnapshot(templateName);
  });
}

function testLoadBrokenTemplateBySnapshot(templateName: string) {
  test(`render ${templateName}`, async () => {
    try {
      loadTemplateFile(templateName);

      throw new Error(`Template ${templateName} should throw an error!`);
    } catch (e) {
      expect(e).toMatchSnapshot(templateName);
    }
  });
}

suite('Jinja', () => {
  beforeAll(async () => {
    native.initJinjaEngine({
      debugInfo: true
    });
    native.clearTemplates();

    loadTemplateFile('.utils.jinja');
    loadTemplateFile('dump_context.yml.jinja');

    for (let i = 1; i < 9; i++) {
      loadTemplateFile(`0${i}.yml.jinja`);
    }
  });

  testTemplateBySnapshot('dump_context.yml.jinja', {
    bool_true: true,
    bool_false: false,
    string: 'test string',
    int: 1,
    float: 3.1415,
    array_int: [9, 8, 7, 6, 5, 0, 1, 2, 3, 4],
    array_bool: [true, false, false, true],
    null: null,
    undefined,
    securityContext: {
      userId: 1,
    }
  });
  testLoadBrokenTemplateBySnapshot('template_error.jinja');

  for (let i = 1; i < 9; i++) {
    testTemplateBySnapshot(`0${i}.yml.jinja`, {});
  }
});
