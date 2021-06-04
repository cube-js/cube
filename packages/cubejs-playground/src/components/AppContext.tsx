import {
  createContext,
  ReactNode,
  useContext,
  useEffect,
  useState,
} from 'react';

export type ContextProps = {
  isDocker?: boolean;
  extDbType?: string;
  setContext: (context: Partial<ContextProps> | null) => void;
};

export type AppContextProps = {
  children: ReactNode;
};

export const AppContext = createContext<ContextProps>({} as ContextProps);

export function AppContextProvider({ children }: AppContextProps) {
  const [context, setContext] = useState<Partial<ContextProps> | null>(null);

  return (
    <AppContext.Provider
      value={{
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
