declare module 'react' {
  import * as ReactTypings from '@types/react';
  export = ReactTypings;

  export function useCallback<T extends (...args: any[]) => any>(
    callback: T,
    deps: DependencyList
  ): T;

  export interface FunctionComponent<P = {}> {
    (props: ReactTypings.PropsWithChildren<P>, context?: any): ReactNode;
  }
}

declare type Nullable<T> = T | null | undefined;
