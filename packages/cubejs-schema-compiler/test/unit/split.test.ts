import fs from 'fs';
import path from 'path';

import { prepareYamlCompiler } from './PrepareCompiler';

describe('Split views', () => {
  async function getSplitJoins(fileName) {
    const content = fs.readFileSync(path.join(process.cwd(), 'test/unit/fixtures', fileName)).toString();

    const model = [{
      fileName,
      content
    }];
  
    const compilers = /** @type Compilers */ prepareYamlCompiler(model);
    await compilers.compiler.compile();

    return compilers.metaTransformer.splitJoins;
  }

  it('post -> post_topic -> topic', async () => {
    const splitJoins = await getSplitJoins('m2m.yml');

    expect(splitJoins.sp).toEqual({
      author: [
        {
          to: 'sp',
          relationship: 'one_to_many'
        }
      ]
    });
  });

  it('ab-s1 [s1 many_to_one a]', async () => {
    const splitJoins = await getSplitJoins('ab-s1-Mx1.yml');

    expect(splitJoins.sp).toEqual({
      s1: [
        {
          to: 'sp',
          relationship: 'many_to_many'
        }
      ]
      
    });
  });

  it('ab-s1 [s1 one_to_many a]', async () => {
    const splitJoins = await getSplitJoins('ab-s1-1xM.yml');

    expect(splitJoins.sp).toEqual({
      s1: [
        {
          to: 'sp',
          relationship: 'one_to_many'
        }
      ]
      
    });
  });

  it('ecom', async () => {
    const splitJoins = await getSplitJoins('ecom.yml');

    expect(splitJoins.orders_view).toEqual({
      orders: [
        {
          to: 'orders_view',
          relationship: 'many_to_one'
        }
      ],
      orders_view: [
        {
          to: 'countries',
          relationship: 'many_to_one'
        }
      ]
    });
  });

  it('transitive1', async () => {
    const splitJoins = await getSplitJoins('transitive1.yml');

    expect(splitJoins.sp).toEqual(
      {
        s1: [
          {
            to: 'sp',
            relationship: 'many_to_many'
          }
        ],
        s2: [
          {
            to: 's1',
            relationship: 'one_to_many'
          }
        ]
      }
    );
  });

  it('route', async () => {
    const splitJoins = await getSplitJoins('route.yml');

    expect(splitJoins.sp).toEqual(
      {
        s1: [
          {
            to: 'sp',
            relationship: 'many_to_many'
          }
        ]
      }
    );
  });

  it('abcd', async () => {
    const splitJoins = await getSplitJoins('abcd.yml');

    expect(splitJoins.sp).toEqual(
      {
        sp: [
          {
            to: 's4',
            relationship: 'many_to_one'
          },
          {
            to: 's3',
            relationship: 'many_to_one'
          }
        ],
        s1: [
          {
            to: 'sp',
            relationship: 'many_to_many'
          }
        ],
        s2: [
          {
            to: 's1',
            relationship: 'one_to_many'
          }
        ]
      }
    );
  });
});
