import { memo, useEffect, useState } from 'react';

import { Icon, IconProps } from './Icon';

export type ArrowProps = {
  /**
   * @default 'bottom'
   */
  direction?: Direction;
  color?: string;
} & IconProps;

type Direction = 'left' | 'right' | 'top' | 'bottom';

const rotationByDirection: Record<Direction, number> = {
  bottom: -180,
  left: -90,
  top: 0,
  right: 90,
};

export const ArrowIcon = memo(function ArrowIcon(props: ArrowProps) {
  const { direction = 'bottom', color, ...iconProps } = props;
  const [rotate, setRotate] = useState(rotationByDirection[direction]);

  useEffect(() => {
    let prevRotate = rotate;
    let nextRotate = rotationByDirection[direction];

    while (prevRotate - nextRotate > 180) {
      nextRotate += 360;
    }

    while (prevRotate - nextRotate < -180) {
      nextRotate -= 360;
    }

    setRotate(nextRotate);
  }, [direction]);

  return (
    <Icon {...iconProps}>
      <svg
        viewBox="0 0 16 16"
        xmlns="http://www.w3.org/2000/svg"
        {...(color ? { style: { color } } : null)}
      >
        <path
          style={{
            transformOrigin: 'center',
            transform: `rotate(${rotate}deg)`,
            transition: 'transform linear var(--transition)',
          }}
          d="M13.772 10.222 9.1 5.55 8 4.45l-1.1 1.1-4.672 4.672a.776.776 0 1 0 1.1 1.1L8 6.649l4.673 4.673a.777.777 0 1 0 1.1-1.1Z"
        />
      </svg>
    </Icon>
  );
});
