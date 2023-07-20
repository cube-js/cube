import fs from 'fs';
import path from 'path';

import * as native from '../js';
import type { JinjaEngine } from '../js';

const suite = native.isFallbackBuild() ? xdescribe : describe;
// TODO(ovr): Find what is going wrong with parallel tests & python on Linux
const darwinSuite = process.platform === 'darwin' && !native.isFallbackBuild() ? describe : xdescribe;

const nativeInstance = new native.NativeInstance();

function loadTemplateFile(engine: native.JinjaEngine, fileName: string): void {
  const content = fs.readFileSync(path.join(process.cwd(), 'test', 'templates', fileName), 'utf8');

  engine.loadTemplate(fileName, content);
}

async function loadPythonCtxFromUtils(fileName: string) {
  const content = fs.readFileSync(path.join(process.cwd(), 'test', 'templates', fileName), 'utf8');
  return nativeInstance.loadPythonContext(
    fileName,
    content
  );
}

function testTemplateBySnapshot(engine: JinjaEngine, templateName: string, ctx: unknown) {
  test(`render ${templateName}`, async () => {
    const actual = engine.renderTemplate(templateName, ctx, null);

    expect(actual).toMatchSnapshot(templateName);
  });
}

function testTemplateWithPythonCtxBySnapshot(engine: JinjaEngine, templateName: string, ctx: unknown) {
  test(`render ${templateName}`, async () => {
    const actual = engine.renderTemplate(templateName, ctx, await loadPythonCtxFromUtils('utils.py'));

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

suite('Python model', () => {
  it('load utils.py', async () => {
    const pythonModule = await loadPythonCtxFromUtils('utils.py');

    expect(pythonModule).toEqual({
      load_data: expect.any(Object),
      load_data_sync: expect.any(Object),
      arg_bool: expect.any(Object),
      arg_sum_integers: expect.any(Object),
      arg_str: expect.any(Object),
      arg_null: expect.any(Object),
    });
  });
});

darwinSuite('Scope Python model', () => {
  it('load scoped-utils.py', async () => {
    const pythonModule = await loadPythonCtxFromUtils('scoped-utils.py');

    expect(pythonModule).toEqual({
      load_data: expect.any(Object),
      load_data_sync: expect.any(Object),
      arg_bool: expect.any(Object),
      arg_sum_integers: expect.any(Object),
      arg_str: expect.any(Object),
      arg_null: expect.any(Object),
    });
  });
});

suite('Jinja', () => {
  const jinjaEngine = nativeInstance.newJinjaEngine({
    debugInfo: true
  });

  beforeAll(async () => {
    loadTemplateFile(jinjaEngine, '.utils.jinja');
    loadTemplateFile(jinjaEngine, 'dump_context.yml.jinja');
    loadTemplateFile(jinjaEngine, 'data-model.yml.jinja');
    loadTemplateFile(jinjaEngine, 'arguments-test.yml.jinja');

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
  testTemplateWithPythonCtxBySnapshot(jinjaEngine, 'data-model.yml.jinja', {});
  testTemplateWithPythonCtxBySnapshot(jinjaEngine, 'arguments-test.yml.jinja', {});

  testLoadBrokenTemplateBySnapshot(jinjaEngine, 'template_error.jinja');

  for (let i = 1; i < 9; i++) {
    testTemplateBySnapshot(jinjaEngine, `0${i}.yml.jinja`, {});
  }
});
