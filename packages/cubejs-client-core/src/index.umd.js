import cube from './index';
import * as clientCoreExports from './index';

Object.keys(clientCoreExports).forEach((key) => {
  cube[key] = clientCoreExports[key];
});

export default cube;
