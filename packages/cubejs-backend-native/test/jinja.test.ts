import fs from 'fs';
import path from 'path';

import * as native from '../js';
import { FileContent } from '@cubejs-backend/shared';

const suite = native.isFallbackBuild() ? xdescribe : describe;

function testTemplateBySnapshot(templateName: string, ctx: unknown) {
  test(`render ${templateName}`, async () => {
    let actual = native.renderTemplate(templateName, ctx);

    expect(actual).toMatchSnapshot(templateName);
  })
}

suite('Jinja', () => {
  function loadTemplateFile(fileName: string): FileContent {
    const content = fs.readFileSync(path.join(process.cwd(), 'test', 'templates', fileName), 'utf8');

    return {
      fileName,
      content
    };
  }

  beforeAll(async () => {
    const templates = [
      loadTemplateFile('.utils.jinja'),
      loadTemplateFile('dump_context.yml.jinja'),
    ];

    for (let i = 1; i < 9; i++) {
      templates.push(
          loadTemplateFile(`0${i}.yml.jinja`)
      );
    }

    native.loadTemplates(templates);
  });

  testTemplateBySnapshot('dump_context.yml.jinja', {
    'bool_true': true,
    'bool_false': false,
    'string': 'test string',
    'int': 1,
    'float': 3.1415,
    'array_int': [9, 8, 7, 6, 5, 0, 1, 2, 3, 4],
    'array_bool': [true, false, false, true],
    securityContext: {
      userId: 1,
    }
  })

  for (let i = 1; i < 9; i++) {
    testTemplateBySnapshot(`0${i}.yml.jinja`, {})
  }
});
