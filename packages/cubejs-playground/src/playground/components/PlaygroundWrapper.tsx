import { ReactNode } from 'react';
import { BrowserRouter } from 'react-router-dom';
import styled from 'styled-components';

import GlobalStyles from '../../components/GlobalStyles';
import {
  SecurityContextProvider,
  SecurityContextProps,
  SecurityContextProviderProps,
} from '../../components/SecurityContext/SecurityContextProvider';
import {
  AppContextProps,
  AppContextProvider,
  PlaygroundContext,
} from '../../components/AppContext';

const StyledWrapper = styled.div`
  background-color: var(--layout-body-background);
  min-height: 100vh;
`;

type PlaygroundWrapperProps = {
  children: ReactNode;
  identifier?: string;
  playgroundContext?: Partial<PlaygroundContext>;
} & Pick<SecurityContextProps, 'token' | 'onTokenPayloadChange'> &
  Pick<SecurityContextProviderProps, 'tokenUpdater'> &
  Pick<AppContextProps, 'apiUrl'>;

export function PlaygroundWrapper({
  token,
  apiUrl,
  identifier,
  playgroundContext,
  children,
  tokenUpdater,
  onTokenPayloadChange,
}: PlaygroundWrapperProps) {
  return (
    <StyledWrapper>
      <BrowserRouter>
        <AppContextProvider
          token={token}
          apiUrl={apiUrl}
          identifier={identifier}
          playgroundContext={playgroundContext || {}}
        >
          <SecurityContextProvider
            tokenUpdater={tokenUpdater}
            onTokenPayloadChange={onTokenPayloadChange}
          >
            {children}
          </SecurityContextProvider>
        </AppContextProvider>

        <GlobalStyles />
      </BrowserRouter>
    </StyledWrapper>
  );
}
