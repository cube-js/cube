import { useContext } from 'react';
import { AppContext } from '../components/AppContext';

export function useAppContext() {
  return useContext(AppContext);
}

export function usePlaygroundContext() {
  const { playgroundContext } =  useAppContext();

  return playgroundContext;
}

export function useIsCloud() {
  const { playgroundContext } = useAppContext();

  return playgroundContext?.isCloud || false;
}

export function useToken() {
  const { token } = useAppContext();

  return token;
}
