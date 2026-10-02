import fs from 'fs';
import path from 'path';

import * as native from '../js';

type InitJinjaFn = () => Promise<{
  pyCtx: native.PythonCtx,
  jinjaEngine: native.JinjaEngine
}>;

const suite = native.isFallbackBuild() ? xdescribe : describe;

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
      load_callable: expect.any(Object),
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

suite('Python memo', () => {
  // globals.py is loaded once per data model compilation, so is memo.py here
  const compile = async () => {
    const pyCtx = await loadPythonCtxFromUtils('memo.py');
    const jinjaEngine = nativeInstance.newJinjaEngine({
      debugInfo: true,
      filters: pyCtx.filters,
      workers: 1,
    });
    loadTemplateFile(jinjaEngine, 'memo.yml.jinja');

    const render = () => jinjaEngine.renderTemplate('memo.yml.jinja', {}, {
      ...pyCtx.variables,
      ...pyCtx.functions,
    });

    // Two model files using the same functions
    return Promise.all([render(), render()]);
  };

  it('calls a memoized function once per arguments within a compilation', async () => {
    const expected = 'sync: 1 1 2\nasync: 1 1 2';

    const first = await compile();
    expect(first.map((r) => r.trim())).toEqual([expected, expected]);

    // A new compilation calls the functions again
    const second = await compile();
    expect(second.map((r) => r.trim())).toEqual([expected, expected]);
  });

  it('drops a compilation\'s cached results with its TemplateContext', async () => {
    const fileName = path.join(process.cwd(), 'test', 'templates', 'memo_leak.py');
    const pyCtx = await nativeInstance.loadPythonContext(fileName, fs.readFileSync(fileName, 'utf8'));
    const jinjaEngine = nativeInstance.newJinjaEngine({ debugInfo: true, filters: pyCtx.filters, workers: 1 });
    loadTemplateFile(jinjaEngine, 'memo_leak.yml.jinja');

    expect((await jinjaEngine.renderTemplate('memo_leak.yml.jinja', {}, { ...pyCtx.functions })).trim()).toEqual('kept: 0');
  });

  it('keeps the cache of a call running while another compilation loads', async () => {
    const fileName = path.join(process.cwd(), 'test', 'templates', 'memo_reload.py');
    const content = fs.readFileSync(fileName, 'utf8');
    const pyCtx = await nativeInstance.loadPythonContext(fileName, content);
    const jinjaEngine = nativeInstance.newJinjaEngine({ debugInfo: true, filters: pyCtx.filters, workers: 1 });
    loadTemplateFile(jinjaEngine, 'memo_reload.yml.jinja');

    const rendering = jinjaEngine.renderTemplate('memo_reload.yml.jinja', {}, { ...pyCtx.functions });
    // load_twice() is waiting between its two calls by now
    await new Promise((resolve) => setTimeout(resolve, 200));
    await nativeInstance.loadPythonContext(fileName, content);

    // One call, its result cached for the second
    expect((await rendering).trim()).toEqual('reload: 11');
  });

  it('gives each compilation its own cache for functions in imported modules', async () => {
    const load = async () => {
      const fileName = path.join(process.cwd(), 'test', 'templates', 'memo_imported.py');
      const pyCtx = await nativeInstance.loadPythonContext(fileName, fs.readFileSync(fileName, 'utf8'));
      const jinjaEngine = nativeInstance.newJinjaEngine({ debugInfo: true, filters: pyCtx.filters, workers: 1 });
      loadTemplateFile(jinjaEngine, 'memo_imported.yml.jinja');

      return async () => (await jinjaEngine.renderTemplate('memo_imported.yml.jinja', {}, { ...pyCtx.functions })).trim();
    };

    // memo_helper.py is imported once, so its call counter is shared, and compilations overlap
    // A method called on a returned object runs every time, so no compilation gets another one's result
    const renderA = await load();
    expect(await renderA()).toEqual('imported: 1 1 1\nmethod: 1\nclients: 11 22 2\nday: 1 1\ncfg: 2 2');
    const renderB = await load();
    expect(await renderA()).toEqual('imported: 1 1 1\nmethod: 2\nclients: 11 22 4\nday: 1 1\ncfg: 2 2');
    expect(await renderB()).toEqual('imported: 2 2 2\nmethod: 3\nclients: 11 22 6\nday: 3 3\ncfg: 4 4');
  });
});
