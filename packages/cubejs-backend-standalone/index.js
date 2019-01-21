const ApiGateway = require('@cubejs-backend/api-gateway');
const PrepareCompiler = require('@cubejs-backend/schema-compiler/compiler/PrepareCompiler');
const CompilerApi = require('./CompilerApi');
const OrchestratorApi = require('./OrchestratorApi');
const FileRepository = require('./FileRepository');

class CubejsStandalone {
  constructor(options) {
    if (
      !options.driverFactory ||
      !options.apiSecret ||
      !options.dbType
    ) {
      throw new Error('driverFactory, apiSecret, dbType are required options');
    }
    this.driverFactory = options.driverFactory;
    this.apiSecret = options.apiSecret;
    this.schemaPath = options.schemaPath || 'schema';
    this.dbType = options.dbType;
    this.logger = options.logger || ((msg, params) => { console.log(`${msg}: ${JSON.stringify(params)}`)});
    this.repository = new FileRepository(this.schemaPath);
  }

  static create(options) {
    return new CubejsStandalone(options);
  }

  async initApp(app) {
    const compilers = await PrepareCompiler.compile(this.repository, { adapter: this.dbType });
    const apiGateway = new ApiGateway(
      this.apiSecret,
      new CompilerApi(compilers, this.dbType),
      new OrchestratorApi(() => this.getDriver(), this.logger),
      this.logger
    );
    apiGateway.initApp(app);
  }

  getDriver() {
    if (!this.driver) {
      this.driver = this.driverFactory();
    }
    return this.driver;
  }
}

module.exports = CubejsStandalone;