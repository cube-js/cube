import { createContext, useState, useEffect, ReactNode, memo } from 'react';
import jwtDecode from 'jwt-decode';

import { SecurityContext } from './SecurityContext';
import { useAppContext, useIsMounted, useLocalStorage } from '../../hooks';

export type SecurityContextProps = {
  payload: string;
  // The token stored in local storage
  token: string | null;
  // Current token that should be used
  currentToken: string | null;
  isModalOpen: boolean;
  setIsModalOpen: any;
  saveToken: (token: string | null) => Promise<void>;
  refreshToken: () => Promise<void>;
  onTokenPayloadChange: (
    payload: Record<string, any>,
    token: string | null
  ) => Promise<string>;
};

export const SecurityContextContext = createContext<SecurityContextProps>({
  payload: '',
  token: null,
  currentToken: null,
  isModalOpen: false,
} as SecurityContextProps);

export type SecurityContextProviderProps = {
  children: ReactNode;
  tokenUpdater?: (token: string | null) => Promise<string | null>;
} & Pick<SecurityContextProps, 'onTokenPayloadChange'>;

let mutex = 0;
let refreshingToken: string | null = null;

export const SecurityContextProvider = memo(function SecurityContextProvider({
  children,
  tokenUpdater,
  onTokenPayloadChange,
}: SecurityContextProviderProps) {
  const isMounted = useIsMounted();
  const [savedToken, setToken, removeToken] = useLocalStorage<string | null>(
    'cubejsToken',
    null
  );
  const { token: appToken, setContext } = useAppContext();

  const [payload, setPayload] = useState('');
  const [isModalOpen, setIsModalOpen] = useState(false);

  const token = savedToken || appToken;

  async function refreshToken(removeSavedToken = false) {
    const tokenToRefresh = removeSavedToken ? appToken : token;

    if (
      tokenToRefresh != null &&
      tokenUpdater &&
      refreshingToken !== tokenToRefresh
    ) {
      refreshingToken = tokenToRefresh;
      const currentMutex = mutex;
      const refreshedToken = await tokenUpdater(tokenToRefresh);

      if (isMounted() && currentMutex === mutex) {
        if (savedToken && removeSavedToken === false) {
          setToken(refreshedToken);
        } else {
          setContext({ token: refreshedToken });
        }

        refreshingToken = null;
        mutex++;
      }
    }
  }

  useEffect(() => {
    if (token) {
      try {
        const payload = jwtDecode(token);
        setPayload(JSON.stringify(payload, null, 2));
      } catch (error) {
        setPayload('');
        console.error('Invalid JWT token', token);
      }
    } else {
      setPayload('');
    }
  }, [token]);

  return (
    <SecurityContextContext.Provider
      value={{
        token: savedToken,
        currentToken: token,
        payload,
        isModalOpen,
        setIsModalOpen,
        async saveToken(token) {
          if (!token) {
            await refreshToken(true);
            removeToken();
          } else {
            setToken(token);
          }
        },
        refreshToken,
        onTokenPayloadChange,
      }}
    >
      {children}
      <SecurityContext />
    </SecurityContextContext.Provider>
  );
});
