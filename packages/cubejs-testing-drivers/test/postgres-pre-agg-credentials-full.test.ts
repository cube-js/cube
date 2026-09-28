import { testQueries } from '../src/tests/testQueries';

testQueries('postgres', {
  extendedEnv: 'pre-agg-credentials',
});
