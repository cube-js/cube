import { CubeIconProps, tasty, UpIcon } from '@cube-dev/ui-kit';
import { memo, useEffect, useState } from 'react';

const StyledUpIcon = tasty(UpIcon, {
  styles: {
    transformOrigin: 'center',
    transition: 'rotate linear 120ms, scale linear 120ms',
  },
});

export type ChevronIconProps = {
  /**
   * @default 'bottom'
   */
  direction?: Direction;
  color?: string;
} & CubeIconProps;

type Direction = 'left' | 'right' | 'top' | 'bottom';

const rotationByDirection: Record<Direction, number> = {
  top: 0,
  right: 90,
  bottom: 180,
  left: 270,
};

function flipRotation(rotation: number) {
  return (rotation + 180) % 360;
}

export const ChevronIcon = memo(function ChevronIcon(props: ChevronIconProps) {
  const { direction = 'bottom', color, ...iconProps } = props;
  const [rotate, setRotate] = useState(rotationByDirection[direction]);
  const [flipScale, setFlipScale] = useState(1); // Tracks flipping: 1 (normal) or -1 (flipped)

  useEffect(() => {
    let nextRotate = rotationByDirection[direction];

    if (flipScale === -1) {
      nextRotate = flipRotation(nextRotate);
    }

    // Check if the change is to the opposite direction
    const isOpposite = Math.abs((rotate - nextRotate) % 360) === 180;

    if (isOpposite) {
      // Toggle the flip state
      setFlipScale((prev) => -prev); // Alternates between 1 and -1
    } else {
      while (rotate - nextRotate > 180) {
        nextRotate += 360;
      }
      while (rotate - nextRotate < -180) {
        nextRotate -= 360;
      }

      setRotate(nextRotate);
    }
  }, [direction]);

  return (
    <StyledUpIcon
      {...iconProps}
      style={{
        rotate: `${rotate}deg`,
        scale: `1 ${flipScale}`,
      }}
    />
  );
});
