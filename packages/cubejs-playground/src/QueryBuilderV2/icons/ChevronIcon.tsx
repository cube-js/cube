import { CubeIconProps, UpIcon } from '@cube-dev/ui-kit';
import { memo, useEffect, useRef, useState } from 'react';

export type ChevronIconProps = {
  /**
   * @default 'bottom'
   */
  direction?: Direction;
  color?: string;
} & CubeIconProps;

type Direction = 'left' | 'right' | 'top' | 'bottom';

const rotationByDirection: Record<Direction, number> = {
  bottom: -180,
  left: -90,
  top: 0,
  right: 90,
};

const rotationByFlippedDirection: Record<Direction, number> = {
  bottom: 0,
  left: 90,
  top: -180,
  right: -90,
};

export const ChevronIcon = memo(function ChevronIcon(props: ChevronIconProps) {
  const { direction = 'bottom', color, ...iconProps } = props;
  const [rotate, setRotate] = useState(rotationByDirection[direction]);
  const [flipScale, setFlipScale] = useState(1); // Tracks flipping: 1 (normal) or -1 (flipped)

  useEffect(() => {
    let nextRotate =
      flipScale === 1 ? rotationByDirection[direction] : rotationByFlippedDirection[direction];

    // Check if the change is to the opposite direction
    const isOpposite = Math.abs(rotate - nextRotate) === 180;

    if (isOpposite) {
      // Toggle the flip state
      setFlipScale((prev) => -prev); // Alternates between 1 and -1
    } else {
      if (flipScale === -1) {
        nextRotate = rotationByFlippedDirection[direction];
      }

      // Calculate the shortest rotation path
      let adjustedNextRotate = nextRotate;
      while (rotate - adjustedNextRotate > 180) {
        adjustedNextRotate += 360;
      }
      while (rotate - adjustedNextRotate < -180) {
        adjustedNextRotate -= 360;
      }

      setRotate(adjustedNextRotate);
    }
  }, [direction]);

  return (
    <UpIcon
      {...iconProps}
      style={{
        transformOrigin: 'center',
        transform: `rotate(${rotate}deg) scaleY(${flipScale})`,
        transition: 'transform linear 120ms',
      }}
    />
  );
});
