/* globals describe,test,expect,jest */

import { GraphQLObjectType } from 'graphql';
import { makeSchema } from '../src/graphql';

const metaConfig = [
  {
    config: {
      name: 'Foo',
      measures: [
        {
          name: 'Foo.bar',
          isVisible: true,
        },
      ],
      dimensions: [
        {
          name: 'Foo.id',
          isVisible: true,
        },
        {
          name: 'Foo.time',
          isVisible: true,
        },
      ],
    },
  },
];

describe('Graphql Schema', () => {
  test('should make valid schema', () => {
    const schema = makeSchema(metaConfig);
    expect(schema).toBeDefined();
    expect(schema.getTypeMap()).toHaveProperty('FooMembers');
    const fooFields = (schema.getType('FooMembers') as GraphQLObjectType).getFields();
    expect(fooFields).toHaveProperty('bar');
    expect(fooFields).toHaveProperty('id');
    expect(fooFields).toHaveProperty('time');
  });

  test('should make valid schema when name is not capitalized', async () => {
    const schema = makeSchema(JSON.parse(
      JSON.stringify(metaConfig)
        .replace(/Foo/g, 'foo')
    ));
    expect(schema).toBeDefined();
    expect(schema.getTypeMap()).toHaveProperty('FooMembers');
    const fooFields = (schema.getType('FooMembers') as GraphQLObjectType).getFields();
    expect(fooFields).toHaveProperty('bar');
    expect(fooFields).toHaveProperty('id');
    expect(fooFields).toHaveProperty('time');
  });
});
