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
      loadTemplateFile('dump_context.jinja'),
    ];

    for (let i = 1; i < 9; i++) {
      templates.push(
          loadTemplateFile(`0${i}.yml.jinja`)
      );
    }

    native.loadTemplates(templates);
  });

  // testTemplateBySnapshot('dump_context.jinja', {
  //   '1_bool_true': true,
  //   '2_bool_false': false,
  // })

  for (let i = 1; i < 9; i++) {
    testTemplateBySnapshot(`0${i}.yml.jinja`, {})
  }
});
