import { CSSProperties } from 'react';
import styled, { css } from 'styled-components';

export const STEP = 8;

type FlexProps = {
  direction?: CSSProperties['flexDirection'];
  wrap?: boolean;
  justifyContent?: CSSProperties['justifyContent'];
  alignItems?: CSSProperties['alignItems'];
  gap?: number;
  margin?: string | number | number[];
};

export const Flex = styled.div<FlexProps>`
  display: flex;
  flex-direction: ${(props) => props.direction || 'row'};
  flex-wrap: ${(props) => (props.wrap ? 'wrap' : null)};
  justify-content: ${(props) => props.justifyContent};
  align-items: ${(props) => props.alignItems};

  ${gap};

  margin: ${margin};
`;

function gap({ gap, direction }: FlexProps) {
  if (gap) {
    if (direction === 'column') {
      return css`
        & > div {
          margin-bottom: ${gap * STEP}px};
        }

        & > div:last-child {
          margin-bottom: 0;
        }
      `;
    }

    return css`
      & > div {
        margin-right: ${gap * STEP}px};
      }

      & > div:last-child {
        margin-right: 0;
      }
    `;
  }

  return '';
}

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
