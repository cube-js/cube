import { CSSProperties } from 'react';
import styled from 'styled-components';

type BoxProps = {
  align?: CSSProperties['alignSelf']
}

export const Box = styled.div<BoxProps>`
  align-self: ${props => props.align}
`
