import { PropsWithChildren } from 'react';
import styled from 'styled-components';

const IconElement = styled.span`
  display: inline-block;
  color: inherit;
  font-style: normal;
  line-height: 0;
  text-align: center;
  text-transform: none;
  text-rendering: optimizeLegibility;
  -webkit-font-smoothing: antialiased;

  & > svg {
    display: block;
    width: 1em;
    height: 1em;

    line-height: 1;
    fill: currentColor;
  }
`;

export type IconProps = React.HTMLAttributes<HTMLSpanElement>;

export function Icon(props: PropsWithChildren<IconProps>) {
  const { children, ...spanProps } = props;

  return (
    <IconElement role="presentation" aria-hidden="true" {...spanProps}>
      {props.children}
    </IconElement>
  );
}
