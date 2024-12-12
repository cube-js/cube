import { testQueries } from '../src/tests/testQueries';

testQueries('postgres', {
  extendedEnv: 'native-cubestore-response'
});
