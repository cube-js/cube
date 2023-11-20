import classnames from 'classnames/bind';
import Image from 'next/image';

import * as styles from './CloudPromoBlock.module.scss';
import cubeCloudIcon from './cube-cloud-icon.svg';

const cn = classnames.bind(styles);

export interface CloudPromoBlockProps {
  children: React.ReactNode;
  // children: string;
}

export const CloudPromoBlock = ({children}: CloudPromoBlockProps) => {
  return (
    <div className={cn('CloudPromoBlock__Wrapper')}>
      <div className={cn('CloudPromoBlock__Logo')}>
        <Image alt="Cube Cloud icon" src={cubeCloudIcon} />
      </div>
      <div>{children}</div>
    </div>
  )
};
