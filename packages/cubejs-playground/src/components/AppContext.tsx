import {
  createContext,
  ReactNode,
  useCallback,
  useEffect,
  useState
} from 'react';
import { useAppContext } from '../hooks';

export type PlaygroundContext = {
  anonymousId: string;
  cubejsToken: string;
  basePath: string;
  isDocker: boolean;
  dbType: string | null;
  telemetry: boolean;
  shouldStartConnectionWizardFlow: boolean;
  dockerVersion: string | null;
  identifier: string;
  previewFeatures: boolean;
  serverCoreVersion: string;
  // @deprecated
  coreServerVersion: string;
  isCloud: boolean;
  livePreview?: boolean;
};

export type SystemContext = {
  basePath: string;
  isDocker: boolean;
  dockerVersion: string | null;
};

export type ContextProps = {
  ready: boolean;
  playgroundContext: Partial<PlaygroundContext>;
  schemaVersion: number;
  apiUrl: string | null;
  token: string | null;
  identifier?: string | null;
  setContext: (context: Partial<ContextProps> | null) => void;
};

export type AppContextProps = {
  children: ReactNode;
} & Partial<Omit<ContextProps, 'setContext'>>;

export const AppContext = createContext<ContextProps>({} as ContextProps);

export function AppContextProvider({
  children,
  ...contextProps
}: AppContextProps) {
  const [context, setContextState] = useState<Partial<ContextProps> | null>(
    contextProps || null
  );
  
  const setContext = useCallback<(context: Partial<ContextProps> | null) => any>((context) => {
    setContextState((currentContext) => ({
      ...currentContext,
      ...context,
      playgroundContext: {
        ...currentContext?.playgroundContext,
        ...context?.playgroundContext,
      }
    }));
  }, []);

  return (
    <AppContext.Provider
      value={{
        apiUrl: null,
        schemaVersion: 0,
        ready: false,
        ...context,
        token: context?.token || null,
        playgroundContext: context?.playgroundContext || {},
        setContext
      }}
    >
      {children}
    </AppContext.Provider>
  );
}

type AppContextConsumerProps = {
  onReady: (context: ContextProps) => void;
};

export function AppContextConsumer({ onReady }: AppContextConsumerProps) {
  const context = useAppContext();

  useEffect(() => {
    onReady(context);
  }, [context]);

  return null;
}
