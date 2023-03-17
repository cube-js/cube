import React, { useEffect, useState } from 'react';
import { Radio } from 'antd';
import Link from 'gatsby-link';
import {
  FRAMEWORKS,
} from '../../stores/frameworkOfChoice';

import * as styles from '../../../static/styles/index.module.scss';

type Props = {
  value: string;
};

// Check if window is defined (so if in the browser or in node.js).
const isBrowser = typeof window !== "undefined"

const FrameworkSwitcher: React.FC<Props> = () => {

  const [framework, setFramework] = useState('vanilla');

  if (isBrowser) {
    useEffect(() => {
      const arrayOfPath = window.location.pathname.split('/');
      const framework = arrayOfPath[arrayOfPath.length - 1];
      const allFrameworks = ['vue', 'react', 'angular']

      setFramework(allFrameworks.includes(framework) ? framework : 'vanilla');
    }, [window.location.pathname]);
  }

  return (
    <Radio.Group className={styles.frameworkSwitcher} value={framework}>
      {FRAMEWORKS.map((framework) => (
        <Link
          key={framework.slug}
          to={`/frontend-introduction/${
            framework.slug === 'vanilla' ? '' : framework.slug
          }`}
        >
          <Radio.Button key={framework.slug} value={framework.slug}>
            {framework.name}
          </Radio.Button>
        </Link>
      ))}
    </Radio.Group>
  );
};

export default FrameworkSwitcher;
