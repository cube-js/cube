import {
  createContext,
  ReactNode,
  useContext,
  useEffect,
  useState,
} from 'react';

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
  livePreview?: boolean;
};

export type SystemContext = {
  basePath: string;
  isDocker: boolean;
  dockerVersion: string | null;
};

export type ContextProps = {
  isCloud: boolean;
  ready: boolean;
  identifier?: string | null;
  playgroundContext?: PlaygroundContext;
  setContext: (context: Partial<ContextProps> | null) => void;
};

export type AppContextProps = {
  children: ReactNode;
} & Omit<ContextProps, 'ready' | 'setContext'>;

export const AppContext = createContext<ContextProps>({} as ContextProps);

export function AppContextProvider({
  isCloud = false,
  children,
  ...contextProps
}: AppContextProps) {
  const [context, setContext] = useState<Partial<ContextProps> | null>(
    contextProps || null
  );

  return (
    <AppContext.Provider
      value={{
        isCloud,
        ready: false,
        ...context,
        setContext(context: Partial<ContextProps> | null) {
          setContext((currentContext) => ({
            ...context,
            ...currentContext,
          }));
        },
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

export function useAppContext() {
  return useContext(AppContext);
}

export function usePlaygroundContext() {
  const { playgroundContext } =  useAppContext();

  return playgroundContext;
}

export function useIsCloud() {
  const { isCloud } =  useAppContext();

  return isCloud;
}
