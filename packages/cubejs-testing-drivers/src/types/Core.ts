// export declare class CubejsServerCore {
//     public readonly systemOptions?: any;
  
//     public static version(): any;

//     public static driverDependencies: any;

//     public static lookupDriverClass: any;

//     public static createDriver: any;

//     public static getDriverMaxPool: any;

//     public readonly repository: any;

//     public devServer: any;

//     public readonly orchestratorStorage: any;

//     public readonly repositoryFactory: any;

//     public contextToDbType: any;

//     public contextToExternalDbType: any;

//     public compilerCache: any;

//     public readonly contextToOrchestratorId: any;

//     public readonly preAggregationsSchema: any;

//     public readonly orchestratorOptions: any;

//     public logger: any;

//     public optsHandler: any;

//     public preAgentLogger: any;

//     public readonly options: any;

//     public readonly contextToAppId: any;

//     public readonly standalone: any;

//     public maxCompilerCacheKeep: any;

//     public scheduledRefreshTimerInterval: any;

//     public driver: any;

//     public apiGatewayInstance: any;

//     public readonly event: any;

//     public projectFingerprint: any;

//     public anonymousId: any;

//     public coreServerVersion: any;

//     public contextAcceptor: any;

//     public constructor(opts?: any, systemOptions?: any);

//     public createContextAcceptor(): any;

//     public isReadyForQueryProcessing(): any;

//     public startScheduledRefreshTimer(): any;

//     public reloadEnvVariables(): any;

//     public initAgent(): any;

//     public flushAgent(): any;

//     public initApp(app: any): any;

//     public initSubscriptionServer(sendMessage: any): any;

//     public initSQLServer(): any;

//     public apiGateway(): any;

//     public createApiGatewayInstance(apiSecret: any, getCompilerApi: any, getOrchestratorApi: any, logger: any, options: any): any;

//     public contextRejectionMiddleware(req: any, res: any, next: any): any;

//     public getCompilerApi(context: any): any;

//     public resetInstanceState(): any;

//     public getOrchestratorApi(context: any): any;

//     public createCompilerApi(repository: any, options?: any): any;

//     public createOrchestratorApi(getDriver: any, options: any): any;

//     public handleScheduledRefreshInterval: (options: any) => any;

//     public getRefreshScheduler(): any;

//     public runScheduledRefresh(context: any, queryingOptions?: any): any;

//     public warningBackgroundContextShow: any;

//     public migrateBackgroundContext(ctx: any): any;

//     public getDriver(context: any, options?: any): any;

//     public resolveDriver(context: any, options?: any): any;

//     public testConnections(): any;

//     public releaseConnections(): any;

//     public beforeShutdown(): any;

//     public causeErrorPromise: any;

//     public onUncaughtException: (e: any) => any;

//     public shutdown(): any;
// }
