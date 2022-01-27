export interface FileContent {
  fileName: string;
  content: string;
  isModule?: boolean;
}

export interface RequestContext {
  // @deprecated Renamed to securityContext, please use securityContext.
  authInfo?: any;
  securityContext: any;
  requestId: string;
}

export interface SchemaFileRepository {
  localPath: () => string;
  dataSchemaFiles: (includeDependencies?: boolean) => Promise<FileContent[]>;
}
