import classNames from 'classnames/bind';
import { CubeLogo } from "../CubeLogo";

import styles from './LogoWithVersion.module.scss';
import { Anchor } from '@/components/overrides/Anchor/Anchor';

const cn = classNames.bind(styles);
const PACKAGE_VERSION = require('../../../../../lerna.json').version;

export const LogoWithVersion = () => {
  return (
    <>
      <Anchor href="https://cube.dev">
        <CubeLogo />
      </Anchor>
      <div className={cn('Version')}>
        {PACKAGE_VERSION}
      </div>
    </>
  )
};
