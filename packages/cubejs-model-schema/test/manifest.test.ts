import { describeModelSchemas } from '@cubejs-backend/schema-compiler';

import { modelSchemaFromJoi } from '../src/converter';

// Every change to the model language shows up here as a diff of the manifest, for review in the
// Core PR. Update with `yarn unit -u` after checking the diff.
describe('the generated manifest', () => {
  const { schema, stats } = modelSchemaFromJoi(describeModelSchemas());

  it('matches the snapshot', () => {
    expect(schema).toMatchSnapshot();
  });

  it('marks the same places where it differs from Core', () => {
    expect(stats).toMatchSnapshot();
  });
});
