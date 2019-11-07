/* globals describe,test,expect,jest */

const AppContainer = require('../dev/templates/AppContainer');

describe(`Templates`, () => {
  jest.setTimeout(600000);
  test(`static`, async () => {
    const appContainer = new AppContainer('test-output/static', [
      'create-react-app', 'react-antd-static', 'credentials', 'bizchart-charts', 'antd-tables'
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
      'create-react-app', 'react-antd-dynamic', 'credentials', 'bizchart-charts', 'antd-tables'
    ], {
      credentials: {
        apiUrl: "http://localhost:4000",
        cubejsToken: "foo"
      }
    });

    await appContainer.applyTemplates();
    await appContainer.ensureDependencies();
  });
});
