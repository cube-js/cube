import React from 'react';
import * as styles from './styles.module.scss';

const PACKAGE_VERSION = require('../../../../lerna.json').version;

export const Version = () => (
  <span className={styles.version}>
    {PACKAGE_VERSION}
  </span>
);
