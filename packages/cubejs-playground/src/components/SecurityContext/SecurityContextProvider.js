import { createContext, useState, useEffect } from 'react';
import jwtDecode from 'jwt-decode';

import SecurityContext from './SecurityContext';

export const SecurityContextContext = createContext({
  claims: null,
  token: null,
  isValid: false,
  isModalOpen: false,
});

export default function SecurityContextProvider({ children }) {
  const [token, setToken] = useState(null);
  const [claims, setClaims] = useState('');
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
        const claims = jwtDecode(token);
        setClaims(JSON.stringify(claims, null, 2));
      } catch (error) {
        console.error('Invalid JWT token');
      }
    }
  }, [token]);

  function saveToken(token, saveToLocalStorage = true) {
    saveToLocalStorage && localStorage.setItem('cubejsToken', token);
    setToken(token);
  }

  return (
    <SecurityContextContext.Provider
      value={{
        claims,
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
