import fs from 'fs';
import path from 'path';

import { CubeToMetaTransformer } from 'src/compiler/CubeToMetaTransformer';
import { prepareYamlCompiler } from './PrepareCompiler';

describe('Switch Dimension', () => {
  it('Switch dimension meta type', async () => {
    const modelContent = fs.readFileSync(
      path.join(process.cwd(), '/test/unit/fixtures/switch-dimension.yml'),
      'utf8'
    );
    const { metaTransformer, compiler } = prepareYamlCompiler(modelContent);
    await compiler.compile();

    const cube = metaTransformer.cubes[0];
    const numberDim = cube.config.dimensions.find((d) => d.name === 'orders.number');
    const statusDim = cube.config.dimensions.find((d) => d.name === 'orders.status');
    const currencyDim = cube.config.dimensions.find((d) => d.name === 'orders.currency');

    expect(numberDim.type).toBe('number');
    expect(statusDim.type).toBe('string');
    expect(currencyDim.type).toBe('string');
  });
});
