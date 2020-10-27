import cubejs from './index';
import * as clientCoreExports from './index';

Object.keys(clientCoreExports).forEach((key) => {
  cubejs[key] = clientCoreExports[key];
});

export default cubejs;
