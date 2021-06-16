import { ReactNode } from 'react';
import { BrowserRouter } from 'react-router-dom';
import styled from 'styled-components';

import GlobalStyles from '../../components/GlobalStyles';
import {
  SecurityContextProvider,
  SecurityContextContextProps,
} from '../../components/SecurityContext/SecurityContextProvider';
import {
  AppContextProvider,
  PlaygroundContext,
} from '../../components/AppContext';

const StyledWrapper = styled.div`
  background-color: var(--layout-body-background);
  min-height: 100vh;
`;

type PlaygroundWrapperProps = {
  identifier?: string;
  playgroundContext?: PlaygroundContext;
  children: ReactNode;
} & Pick<SecurityContextContextProps, 'getToken'>;

export function PlaygroundWrapper({
  identifier,
  playgroundContext,
  getToken,
  children,
}: PlaygroundWrapperProps) {
  return (
    <StyledWrapper>
      <BrowserRouter>
        <AppContextProvider
          identifier={identifier}
          playgroundContext={playgroundContext}
        >
          <SecurityContextProvider getToken={getToken}>
            {children}
          </SecurityContextProvider>
        </AppContextProvider>

        <GlobalStyles />
      </BrowserRouter>
    </StyledWrapper>
  );
}
