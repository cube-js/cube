import { ReactNode } from 'react';
import { BrowserRouter } from 'react-router-dom';
import styled from 'styled-components';

import GlobalStyles from '../components/GlobalStyles';
import {
  SecurityContextProvider,
  TSecurityContextContextProps,
} from '../components/SecurityContext/SecurityContextProvider';

const StyledWrapper = styled.div`
  background-color: var(--layout-body-background);
  min-height: 100vh;
`;

type TPlaygroundWrapperProps = {
  tokenKey?: string;
  children: ReactNode;
} & Pick<TSecurityContextContextProps, 'getToken'>;

export default function PlaygroundWrapper({
  tokenKey,
  getToken,
  children,
}: TPlaygroundWrapperProps) {
  return (
    <StyledWrapper>
      <BrowserRouter>
        <SecurityContextProvider tokenKey={tokenKey} getToken={getToken}>
          {children}
        </SecurityContextProvider>

        <GlobalStyles />
      </BrowserRouter>
    </StyledWrapper>
  );
}
