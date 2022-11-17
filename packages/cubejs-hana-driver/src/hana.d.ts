declare module 'types-hana-client' {
    type HanaClientCallBack = (error: Error) => void;
    type HanaResultCallBack<T> = (error: Error, results: T) => void;

    // tslint:disable-next-line:interface-name
    interface ConnectionOptions {
        host?: string;
        port?: number;
        serverNode?: string;
        uid?: string;
        pwd?: string;
        schema?: string;
        databaseName?: string;
        autoCommit?: boolean;
        ca?: string;
        encrypt?: boolean,
        sslValidateCertificate?: boolean,
    }

    type Results = [];

    class ResultSet {
      public next(): boolean;

      public getValues<T>(): T;
    }

    type HanaResultSetCallBack<T> = (error: Error, rs: ResultSet) => void;

    class Statement {
      public exec<T>(fn: HanaResultCallBack<T>): void;

      public exec<T>(params: any | any[], fn: HanaResultCallBack<T>): void;

      public execBatch<T>(params: any[], fn: HanaResultCallBack<T>): void;

      public execQuery<T>(params: any | any[], fn: HanaResultSetCallBack<T>): void;

      public drop(fn: HanaClientCallBack): void;
    }

    type HanaStatementCallBack = (error: Error, stmt: Statement) => void;

    class Connection {
      public connect(options: ConnectionOptions, fn: HanaClientCallBack): void;

      public disconnect(fn: HanaClientCallBack): void;

      public exec<T>(sql: string, fn: HanaResultCallBack<T>): void;

      public exec<T>(sql: string, params: any | any[], fn: HanaResultCallBack<T>): void;

      public prepare(sql: string, fn: HanaStatementCallBack): void;

      public setAutoCommit(autoCommit: boolean): void;

      public commit(fn: HanaClientCallBack): void;

      public rollback(fn: HanaClientCallBack): void;
    }

    function createConnection(options?: ConnectionOptions): Connection;

    export interface FieldInfo {
      columnName: string,
      originalColumnName: string,
      tableName: string,
      ownerName: string,
      type: number,
      typeName: string,
      nativeType: number,
      nativeTypeName: string,
      precision: number,
      scale: number,
      nullable: number
    }
}
