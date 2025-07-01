import { CheckOutlined } from '@ant-design/icons';
import { Styles, tasty, CopyIcon as CopyIconUIKit } from '@cube-dev/ui-kit';
import { memo, useRef } from 'react';

import { useDebouncedCallback } from '../hooks';

export type CopyIconProps = {
  isCopied: boolean;
  onCopyAnimationEnd: () => void;
};

const Wrapper = tasty({
  styles: {
    display: 'inline-block',
    position: 'relative',
    width: '1em',
    height: '1em',
  },
});

const copyIconStyle: Styles = {
  display: 'grid',
  position: 'absolute',
  left: '50%',
  top: '50%',
  translate: '-50% -50%',
  transitionProperty: 'opacity, rotate',
  transitionDuration: '0.25s',
  transitionTimingFunction: 'ease-out',
};

const CopyIconElement = tasty({
  styles: {
    ...copyIconStyle,
    opacity: {
      '': 1,
      copied: 0,
    },
    rotate: {
      '': '0deg',
      copied: '90deg',
    },
    transitionDelay: {
      '': '0s',
      copied: '0.1s',
    },
  },
  children: <CopyIconUIKit />,
});

const CopiedIconElement = tasty({
  styles: {
    ...copyIconStyle,
    opacity: {
      '': 0,
      copied: 1,
    },
    rotate: {
      '': '-90deg',
      copied: '0deg',
    },
    transitionDelay: {
      '': '0.1s',
      copied: '0s',
    },
  },
  children: <CheckOutlined />,
});

export const CopyIcon = memo(function CopyIcon(props: CopyIconProps) {
  const { isCopied, onCopyAnimationEnd } = props;
  const copyIconRef = useRef<HTMLSpanElement>(null);
  const copiedIconRef = useRef<HTMLSpanElement>(null);
  const dOnCopyAnimationEnd = useDebouncedCallback(
    () => onCopyAnimationEnd(),
    [onCopyAnimationEnd],
    1000
  );

  return (
    <Wrapper>
      <CopyIconElement ref={copyIconRef} mods={{ copied: isCopied }} />
      <CopiedIconElement
        ref={copiedIconRef}
        mods={{ copied: isCopied }}
        onTransitionEnd={dOnCopyAnimationEnd}
      />
    </Wrapper>
  );
});
