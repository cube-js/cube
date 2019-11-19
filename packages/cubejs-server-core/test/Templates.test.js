/* globals describe,test,expect,jest */

const AppContainer = require('../dev/templates/AppContainer');

describe(`Templates`, () => {
  jest.setTimeout(600000);
  test(`static`, async () => {
    const appContainer = new AppContainer('test-output/static', [
      'create-react-app', 'react-antd-static', 'credentials', 'bizcharts-charts', 'antd-tables'
    ], {
      credentials: {
        apiUrl: "http://localhost:4000",
        cubejsToken: "foo"
      }
    });

    await appContainer.applyTemplates();
  });

  test(`dynamic`, async () => {
    const appContainer = new AppContainer('test-output/dynamic', [
      'create-react-app', 'react-antd-dynamic', 'credentials', 'bizcharts-charts', 'antd-tables'
    ], {
      credentials: {
        apiUrl: "http://localhost:4000",
        cubejsToken: "foo"
      }
    });

    await appContainer.applyTemplates();
    await appContainer.ensureDependencies();
  });

  test(`web-socket-transport`, async () => {
    const appContainer = new AppContainer('test-output/real-time', [
      'create-react-app',
      'react-antd-dynamic',
      'credentials',
      'bizcharts-charts',
      'antd-tables',
      'web-socket-transport'
    ], {
      credentials: {
        apiUrl: "http://localhost:4000",
        cubejsToken: "foo"
      }
    });

    await appContainer.applyTemplates();
  });
});
