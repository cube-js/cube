import { createContext, useState, useEffect, useCallback } from 'react';
import jwtDecode from 'jwt-decode';

import SecurityContext from './SecurityContext';

export const SecurityContextContext = createContext({
  payload: null,
  token: null,
  isValid: false,
  isModalOpen: false,
});

export default function SecurityContextProvider({ children }) {
  const [token, setToken] = useState(null);
  const [payload, setPayload] = useState('');
  const [isModalOpen, setIsModalOpen] = useState(false);

  useEffect(() => {
    const token = localStorage.getItem('cubejsToken');
    if (token) {
      setToken(token);
    }
  }, []);

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

  const saveToken = useCallback((token, saveToLocalStorage = true) => {
    saveToLocalStorage && localStorage.setItem('cubejsToken', token);
    setToken(token);
  }, []);

  return (
    <SecurityContextContext.Provider
      value={{
        payload,
        token,
        isValid: false,
        isModalOpen,
        setIsModalOpen,
        saveToken,
      }}
    >
      {children}
      <SecurityContext />
    </SecurityContextContext.Provider>
  );
}
