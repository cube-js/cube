import { CSSProperties } from 'react';
import styled from 'styled-components';

type BoxProps = {
  align?: CSSProperties['alignSelf']
  grow?: CSSProperties['flexGrow']
}

export const Box = styled.div<BoxProps>`
  align-self: ${props => props.align};
  flex-grow: ${props => props.grow};
`
