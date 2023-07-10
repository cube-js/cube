import fs from 'fs';
import path from 'path';

import * as native from '../js';
import { JinjaEngine, newJinjaEngine } from '../js';

const suite = native.isFallbackBuild() ? xdescribe : describe;

function loadTemplateFile(engine: native.JinjaEngine, fileName: string): void {
  const content = fs.readFileSync(path.join(process.cwd(), 'test', 'templates', fileName), 'utf8');

  engine.loadTemplate(fileName, content);
}

function testTemplateBySnapshot(engine: JinjaEngine, templateName: string, ctx: unknown) {
  test(`render ${templateName}`, async () => {
    const actual = engine.renderTemplate(templateName, ctx);

    expect(actual).toMatchSnapshot(templateName);
  });
}

function testLoadBrokenTemplateBySnapshot(engine: JinjaEngine, templateName: string) {
  test(`render ${templateName}`, async () => {
    try {
      loadTemplateFile(engine, templateName);

      throw new Error(`Template ${templateName} should throw an error!`);
    } catch (e) {
      expect(e).toMatchSnapshot(templateName);
    }
  });
}

suite('Jinja', () => {
  const jinjaEngine = native.newJinjaEngine({
    debugInfo: true
  });

  beforeAll(async () => {
    loadTemplateFile(jinjaEngine, '.utils.jinja');
    loadTemplateFile(jinjaEngine, 'dump_context.yml.jinja');

    for (let i = 1; i < 9; i++) {
      loadTemplateFile(jinjaEngine, `0${i}.yml.jinja`);
    }
  });

  testTemplateBySnapshot(jinjaEngine, 'dump_context.yml.jinja', {
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
  testLoadBrokenTemplateBySnapshot(jinjaEngine, 'template_error.jinja');

  for (let i = 1; i < 9; i++) {
    testTemplateBySnapshot(jinjaEngine, `0${i}.yml.jinja`, {});
  }
});
