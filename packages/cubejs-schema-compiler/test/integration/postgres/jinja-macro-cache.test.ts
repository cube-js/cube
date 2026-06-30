import { LRUCache } from 'lru-cache';
import { FileContent, isNativeSupported } from '@cubejs-backend/shared';

import { prepareCompiler } from '../../../src/compiler/PrepareCompiler';

const suite = isNativeSupported() === true ? describe : xdescribe;

const cubeFile = (name: string, extraDimsBlock: string): string => `{% import 'macros.yml' as macros %}

cubes:
  - name: ${name}
    sql: >
      SELECT 1 AS id

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
${extraDimsBlock}
`;

const macroFile = (dimensionName: string) => `{% macro dimensions() %}
      - name: ${dimensionName}
        sql: ${dimensionName}
        type: string
{% endmacro %}
`;

async function compileWith(files: FileContent[], compiledJinjaCache: LRUCache<string, string>) {
  const repo = {
    localPath: () => __dirname,
    dataSchemaFiles: () => Promise.resolve(files),
  };

  const { compiler, metaTransformer } = prepareCompiler(repo, {
    adapter: 'postgres',
    compiledJinjaCache,
  } as any);

  await compiler.compile();

  return { metaTransformer };
}

function dimensionNames(metaTransformer: any, cubeName: string): string[] {
  const cube = metaTransformer.cubes.find((c: any) => c.config.name === cubeName);
  return cube.config.dimensions.map((d: any) => d.name);
}

suite('Jinja macro cache invalidation', () => {
  it('invalidates the cube file render cache when a macro file changes (CUB-2357)', async () => {
    const sharedCache = new LRUCache<string, string>({ max: 250 });

    const filesV1: FileContent[] = [
      { fileName: 'orders.yml', content: cubeFile('orders', '{{ macros.dimensions() }}') },
      { fileName: 'macros.yml', content: macroFile('status') },
    ];

    const v1 = await compileWith(filesV1, sharedCache);
    expect(dimensionNames(v1.metaTransformer, 'orders')).toEqual(['orders.id', 'orders.status']);

    const filesV2: FileContent[] = [
      filesV1[0],
      { fileName: 'macros.yml', content: macroFile('priority') },
    ];

    const v2 = await compileWith(filesV2, sharedCache);
    expect(dimensionNames(v2.metaTransformer, 'orders')).toEqual(['orders.id', 'orders.priority']);
  });

  it('reuses the render cache for unchanged cube files when a sibling cube file changes', async () => {
    const sharedCache = new LRUCache<string, string>({ max: 250 });

    const filesV1: FileContent[] = [
      { fileName: 'orders.yml', content: cubeFile('orders', '') },
      { fileName: 'products.yml', content: cubeFile('products', '') },
      { fileName: 'macros.yml', content: macroFile('unused') },
    ];
    await compileWith(filesV1, sharedCache);

    const cacheSizeAfterFirstCompile = sharedCache.size;

    const filesV2: FileContent[] = [
      {
        fileName: 'orders.yml',
        content: cubeFile('orders', '      - name: status\n        sql: status\n        type: string\n'),
      },
      filesV1[1],
      filesV1[2],
    ];
    await compileWith(filesV2, sharedCache);

    // Only the changed orders.yml should miss the cache; products.yml and
    // macros.yml are byte-identical and the macros fingerprint is unchanged.
    expect(sharedCache.size).toBe(cacheSizeAfterFirstCompile + 1);
  });
});
