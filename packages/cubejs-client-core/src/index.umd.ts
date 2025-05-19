import cube, * as clientCoreExports from './index';

const cubeAll: any = cube;

Object.keys(clientCoreExports).forEach((key) => {
  cubeAll[key] = (clientCoreExports as Record<string, any>)[key];
});

export default cubeAll;
