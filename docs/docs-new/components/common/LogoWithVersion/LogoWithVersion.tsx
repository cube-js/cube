import classNames from 'classnames/bind';
import { CubeLogo } from "../CubeLogo";

import styles from './LogoWithVersion.module.scss';

const cn = classNames.bind(styles);
const PACKAGE_VERSION = require('../../../../../lerna.json').version;

export const LogoWithVersion = () => {
  return (
    <>
      <CubeLogo />
      <div className={cn('Version')}>
        {PACKAGE_VERSION}
      </div>
    </>
  )
};
