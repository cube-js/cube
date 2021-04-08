import { ReactNode, useLayoutEffect } from 'react';
import { BrowserRouter } from 'react-router-dom';
import { CubeProvider } from '@cubejs-client/react';
import styled from 'styled-components';

import { useCubejsApi } from '../hooks';
import GlobalStyles from '../components/GlobalStyles';
import {
  SecurityContextProvider,
  TSecurityContextContextProps,
} from '../components/SecurityContext/SecurityContextProvider';

const StyledWrapper = styled.div`
  background-color: #f3f3fc;
  min-height: 100vh;
`

type TPlaygroundWrapperProps = {
  apiUrl?: string;
  token?: string;
  tokenKey?: string;
  children: ReactNode
} & Pick<TSecurityContextContextProps, 'getToken'>

export default function PlaygroundWrapper({
  apiUrl,
  token,
  tokenKey,
  getToken,
  children,
}: TPlaygroundWrapperProps) {
  const cubejsApi = useCubejsApi(apiUrl, token);

  useLayoutEffect(() => {
    if (apiUrl && token) {
      // @ts-ignore
      window.__cubejsPlayground = {
        // @ts-ignore
        ...window.__cubejsPlayground,
        apiUrl,
        token,
      };
    }
  }, [apiUrl, token]);

  if (!cubejsApi) {
    return null;
  }

  return (
    <StyledWrapper>
      <BrowserRouter>
        <CubeProvider cubejsApi={cubejsApi}>
          <SecurityContextProvider tokenKey={tokenKey} getToken={getToken}>
            {children}
          </SecurityContextProvider>
        </CubeProvider>

        <GlobalStyles />
      </BrowserRouter>
    </StyledWrapper>
  );
}
