import { CSSProperties } from 'react';
import styled from 'styled-components';

export const STEP = 8;

type FlexProps = {
  direction?: CSSProperties['flexDirection'];
  justifyContent?: CSSProperties['justifyContent'];
  gap?: number;
  margin?: string | number | number[];
};

export const Flex = styled.div<FlexProps>`
  display: flex;
  flex-direction: ${(props) => props.direction || 'row'};
  justify-content: ${(props) => props.justifyContent || 'space-between'};
  gap: ${(props) => (props.gap ? `${props.gap * STEP}px` : null)};
  margin: ${margin};
`;

function margin(props: FlexProps) {
  const value = props.margin;

  if (typeof value === 'string') {
    return value;
  }

  if (typeof value === 'number') {
    return `${value * STEP}px`;
  }

  if (Array.isArray(value)) {
    const { length } = value;
    if (length === 2) {
      const [horizontal, vertical] = value;

      return `${horizontal * STEP}px ${vertical * STEP}px`;
    } else if (length > 2) {
      return value
        .fill(0, 0, 4)
        .map((v) => `${v * STEP}`)
        .join(' ');
    }
  }

  if (value && Object.keys(value || {}).length) {
    return Object.entries(value).map(([key, value]) => {
      if (key === 'top') {
        return `${margin({ margin: value })} 0 0 0`;
      } else if (key === 'bottom') {
        return `0 0 ${margin({ margin: value })} 0`;
      }
    });
  }

  return null;
}
