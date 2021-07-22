import cn from 'classnames';
import GatsbyLink from 'gatsby-link';
import React, { useEffect, useRef, useState } from 'react';

import * as styles from './styles.module.scss';
import cubeJsLogo from './cube.js-logo.svg';
import cubeCloudLogo from './cube-cloud-logo.svg';
import cubeCloudLogoSmall from './cube-cloud-logo-sm.svg';

export const DocsSwitcher = () => {
  const [isCloudPage, setIsCloudPage] = useState(false);
  const [isSmall, setIsSmall] = useState(false);
  const [width, setWidth] = useState(0);

  // eslint-disable-next-line no-undef
  const switchWrapperRef = useRef<HTMLDivElement>(null);

  // eslint-disable-next-line no-undef
  const pathName = (typeof window !== "undefined" ? window.location.pathname : '/');

  useEffect(() => {
    setIsCloudPage(pathName.includes('/cloud'));
  }, [isCloudPage, pathName]);

  useEffect(() => {
    const handleResize = () => {
      if (switchWrapperRef.current) {
        const boundingBoxWidth = switchWrapperRef.current.getBoundingClientRect().width;
        setWidth(boundingBoxWidth);
        setIsSmall(boundingBoxWidth < 360);
      }
    };
    // eslint-disable-next-line no-unused-expressions,no-undef
    window.addEventListener('resize', handleResize);
    return () => {
      // eslint-disable-next-line no-unused-expressions,no-undef
      window.removeEventListener('resize', handleResize);
    };
  }, [width]);

  const cloudLogo = React.useMemo(
    () => (isSmall ? cubeCloudLogoSmall : cubeCloudLogo),
    [isSmall],
  );

  const cubejsClasses = cn('ant-btn', { active: !isCloudPage });
  const cubeCloudClasses = cn('ant-btn', { active: isCloudPage });

  return (
    <div ref={switchWrapperRef} className={styles.docsSwitcher}>
      <GatsbyLink to={'/'} className={cubejsClasses} activeClassName="active">
        <img src={cubeJsLogo} alt='Cube.js Logo' />
      </GatsbyLink>
      <GatsbyLink to={'/cloud'} className={cubeCloudClasses} activeClassName="active">
        <img src={cloudLogo} alt='Cube Cloud Logo' />
      </GatsbyLink>
    </div>
  );
};
