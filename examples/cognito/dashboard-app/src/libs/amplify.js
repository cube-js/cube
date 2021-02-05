import React, { useEffect, useState } from 'react';
import { Auth } from '@aws-amplify/auth';
import { Hub } from '@aws-amplify/core';

const def = {
  status: null,
};

const AmplifyContext = React.createContext(def);

export const useAmplify = () => React.useContext(AmplifyContext);

export const AmplifyProvider = ({ children }) => {
  const [userData, setUserData] = useState(null);
  const [authenticated, setAuthenticated] = useState(false);
  const [initializing, setInitializing] = useState(true);

  useEffect(() => {
    const getState = async () => {
      try {
        const userData = await Auth.currentAuthenticatedUser();

        setUserData(userData);
        setAuthenticated(true);
        setInitializing(false);
      } catch (e) {
        if (e === 'The user is not authenticated') {
          setInitializing(false);
          setAuthenticated(false);
        } else {
          console.log('getState error', e);
        }
      }
    };

    const listener = Hub.listen('auth', async (e) => {
      switch (e.payload.event) {
        case 'cognitoHostedUI':
        case 'signIn':
        case 'signUp':
          getState();
          break;
        case 'signOut':
          setUserData(null);
          setAuthenticated(false);
          setInitializing(false);
          break;
        default:
          console.log(e.payload.event, e.payload);
          break;
      }
    });

    getState();

    return listener;
  }, []);

  return (
    <AmplifyContext.Provider value={{
      userData,
      initializing,
      authenticated,
      logout: () => Auth.signOut({ global: false, }),
    }}>
      {children}
    </AmplifyContext.Provider>
  );
};
