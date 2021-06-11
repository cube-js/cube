import {
  createContext,
  useState,
  useEffect,
  ReactNode,
} from 'react';
import jwtDecode from 'jwt-decode';

import { SecurityContext } from './SecurityContext';
import { useLocalStorage } from '../../hooks';

export type SecurityContextContextProps = {
  payload: string;
  token: string | null;
  isModalOpen: boolean;
  setIsModalOpen: any;
  saveToken: (token: string | null) => void;
  getToken: (payload: string) => Promise<string>;
};

export const SecurityContextContext =
  createContext<SecurityContextContextProps>(
    {} as SecurityContextContextProps
  );

type SecurityContextProviderProps = {
  children: ReactNode;
} & Pick<SecurityContextContextProps, 'getToken'>;

export function SecurityContextProvider({
  children,
  getToken,
}: SecurityContextProviderProps) {
  const [token, setToken, removeToken] = useLocalStorage<string | null>(
    'cubejsToken',
    null
  );
  const [payload, setPayload] = useState('');
  const [isModalOpen, setIsModalOpen] = useState(false);

  useEffect(() => {
    if (token) {
      try {
        const payload = jwtDecode(token);
        setPayload(JSON.stringify(payload, null, 2));
      } catch (error) {
        setPayload('');
        console.error('Invalid JWT token');
      }
    } else {
      setPayload('');
    }
  }, [token]);

  return (
    <SecurityContextContext.Provider
      value={{
        token,
        payload,
        isModalOpen,
        setIsModalOpen,
        saveToken: (token) => {
          if (!token) {
            removeToken();
          } else {
            setToken(token);
            console.log('will set', token)
          }
        },
        getToken,
      }}
    >
      {children}
      <SecurityContext />
    </SecurityContextContext.Provider>
  );
}
