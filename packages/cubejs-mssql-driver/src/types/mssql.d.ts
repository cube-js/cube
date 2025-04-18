import 'mssql';

// Because "@types/mssql": "^9.1.7" (latest as of Apr 2025) still doesn't have info about valueHandler
declare module 'mssql' {
  namespace valueHandler {
    function set(
      type: any,
      handler: (value: unknown) => unknown
    ): void;
  }

  export const valueHandler: typeof valueHandler;
}
