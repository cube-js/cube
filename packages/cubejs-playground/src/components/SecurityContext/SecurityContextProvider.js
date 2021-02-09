import { createContext, useState, useEffect, useCallback } from 'react';
import jwtDecode from 'jwt-decode';

import SecurityContext from './SecurityContext';

export const SecurityContextContext = createContext({
  payload: null,
  token: null,
  isValid: false,
  isModalOpen: false,
});

export default function SecurityContextProvider({
  children,
  getToken,
  tokenKey = null,
}) {
  const [token, setToken] = useState(null);
  const [payload, setPayload] = useState('');
  const [isModalOpen, setIsModalOpen] = useState(false);

  const tokenName = tokenKey ? `cubejsToken:${tokenKey}` : 'cubejsToken';

  useEffect(() => {
    const token = localStorage.getItem(tokenName);
    if (token) {
      setToken(token);
    }
  }, [tokenName]);

  useEffect(() => {
    if (token) {
      try {
        const payload = jwtDecode(token);
        setPayload(JSON.stringify(payload, null, 2));
      } catch (error) {
        setPayload('');
        console.error('Invalid JWT token');
      }
    }
  }, [token]);

  const saveToken = useCallback((token) => {
    if (token) {
      localStorage.setItem(tokenName, token);
    } else {
      localStorage.removeItem(tokenName);
      setPayload('');
    }
    setToken(token || null);
  }, [tokenName]);

  return (
    <SecurityContextContext.Provider
      value={{
        payload,
        token,
        isValid: false,
        isModalOpen,
        setIsModalOpen,
        saveToken,
        getToken,
      }}
    >
      {children}
      <SecurityContext />
    </SecurityContextContext.Provider>
  );
}
