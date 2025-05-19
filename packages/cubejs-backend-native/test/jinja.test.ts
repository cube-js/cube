import fs from 'fs';
import path from 'path';

import * as native from '../js';

type InitJinjaFn = () => Promise<{
  pyCtx: native.PythonCtx,
  jinjaEngine: native.JinjaEngine
}>;

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
  const ctx = await nativeInstance.loadPythonContext(
    fileName,
    content
  );

  // console.debug(ctx);

  return ctx;
}

function testTemplateBySnapshot(init: InitJinjaFn, templateName: string, ctx: unknown) {
  test(`render ${templateName}`, async () => {
    const { jinjaEngine } = await init();
    const actual = await jinjaEngine.renderTemplate(templateName, ctx, null);

    expect(actual).toMatchSnapshot(templateName);
  });
}

function testTemplateWithPythonCtxBySnapshot(init: InitJinjaFn, templateName: string, ctx: unknown) {
  test(`render ${templateName}`, async () => {
    const { jinjaEngine, pyCtx } = await init();
    const actual = await jinjaEngine.renderTemplate(templateName, ctx, {
      ...pyCtx.variables,
      ...pyCtx.functions,
    });

    expect(actual).toMatchSnapshot(templateName);
  });
}

function testTemplateErrorWithPythonCtxBySnapshot(init: InitJinjaFn, templateName: string, ctx: unknown) {
  test(`render ${templateName}`, async () => {
    const { jinjaEngine, pyCtx } = await init();

    try {
      await jinjaEngine.renderTemplate(templateName, ctx, {
        ...pyCtx.variables,
        ...pyCtx.functions,
      });

      throw new Error(`Template ${templateName} should throw an error!`);
    } catch (e) {
      expect(e).toMatchSnapshot(templateName);
    }
  });
}

function testLoadBrokenTemplateBySnapshot(init: InitJinjaFn, templateName: string) {
  test(`render ${templateName}`, async () => {
    try {
      const { jinjaEngine } = await init();
      loadTemplateFile(jinjaEngine, templateName);

      throw new Error(`Template ${templateName} should throw an error!`);
    } catch (e) {
      expect(e).toMatchSnapshot(templateName);
    }
  });
}

suite('Python model', () => {
  it('load jinja-instance.py', async () => {
    const pythonModule = await loadPythonCtxFromUtils('jinja-instance.py');

    expect(pythonModule.functions).toEqual({
      load_data: expect.any(Object),
      load_data_sync: expect.any(Object),
      arg_bool: expect.any(Object),
      arg_kwargs: expect.any(Object),
      arg_named_arguments: expect.any(Object),
      arg_sum_integers: expect.any(Object),
      arg_str: expect.any(Object),
      arg_null: expect.any(Object),
      arg_sum_tuple: expect.any(Object),
      arg_sum_map: expect.any(Object),
      arg_seq: expect.any(Object),
      new_int_tuple: expect.any(Object),
      new_str_tuple: expect.any(Object),
      new_safe_string: expect.any(Object),
      new_object_from_dict: expect.any(Object),
      load_class_model: expect.any(Object),
      throw_exception: expect.any(Object),
    });

    expect(pythonModule.variables).toEqual({
      var1: 'test string',
      var2: true,
      var3: false,
      var4: undefined,
      var5: { obj_key: 'val' },
      var6: [1, 2, 3, 4, 5, 6],
      var7: [6, 5, 4, 3, 2, 1],
    });
  });
});

suite('Jinja (new api)', () => {
  const initJinjaEngine: InitJinjaFn = (() => {
    let pyCtx: native.PythonCtx;
    let jinjaEngine: native.JinjaEngine;

    return async () => {
      if (pyCtx && jinjaEngine) {
        return {
          pyCtx,
          jinjaEngine
        };
      }

      pyCtx = await loadPythonCtxFromUtils('jinja-instance.py');
      jinjaEngine = nativeInstance.newJinjaEngine({
        debugInfo: true,
        filters: pyCtx.filters,
        workers: 1,
      });

      return {
        pyCtx,
        jinjaEngine
      };
    };
  })();

  beforeAll(async () => {
    const { jinjaEngine } = await initJinjaEngine();

    loadTemplateFile(jinjaEngine, '.utils.jinja');
    loadTemplateFile(jinjaEngine, 'dump_context.yml.jinja');
    loadTemplateFile(jinjaEngine, 'class-model.yml.jinja');
    loadTemplateFile(jinjaEngine, 'data-model.yml.jinja');
    loadTemplateFile(jinjaEngine, 'arguments-test.yml.jinja');
    loadTemplateFile(jinjaEngine, 'python.yml');
    loadTemplateFile(jinjaEngine, 'variables.yml.jinja');
    loadTemplateFile(jinjaEngine, 'filters.yml.jinja');
    loadTemplateFile(jinjaEngine, 'template_error_python.jinja');

    for (let i = 1; i < 9; i++) {
      loadTemplateFile(jinjaEngine, `0${i}.yml.jinja`);
    }
  });

  testTemplateBySnapshot(initJinjaEngine, 'dump_context.yml.jinja', {
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

  // todo(ovr): Fix issue with tests
  // testTemplateWithPythonCtxBySnapshot(jinjaEngine, 'class-model.yml.jinja', {}, utilsFile);
  testTemplateWithPythonCtxBySnapshot(initJinjaEngine, 'data-model.yml.jinja', {});
  testTemplateWithPythonCtxBySnapshot(initJinjaEngine, 'arguments-test.yml.jinja', {});
  testTemplateWithPythonCtxBySnapshot(initJinjaEngine, 'python.yml', {});
  testTemplateWithPythonCtxBySnapshot(initJinjaEngine, 'variables.yml.jinja', {});
  testTemplateWithPythonCtxBySnapshot(initJinjaEngine, 'filters.yml.jinja', {});
  testTemplateErrorWithPythonCtxBySnapshot(initJinjaEngine, 'template_error_python.jinja', {});

  testLoadBrokenTemplateBySnapshot(initJinjaEngine, 'template_error_syntax.jinja');

  for (let i = 1; i < 9; i++) {
    testTemplateBySnapshot(initJinjaEngine, `0${i}.yml.jinja`, {});
  }
});
