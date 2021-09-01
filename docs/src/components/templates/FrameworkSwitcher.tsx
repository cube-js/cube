import React, { useEffect, useState } from 'react';
import { Radio } from 'antd';
import { RadioChangeEvent } from 'antd/lib/radio/interface';
// import { navigate } from 'gatsby';
import Link from 'gatsby-link';
import {
  useFrameworkOfChoice,
  FRAMEWORKS,
} from '../../stores/frameworkOfChoice';

import * as styles from '../../../static/styles/index.module.scss';

type Props = {
  value: string;
};

const FrameworkSwitcher: React.FC<Props> = () => {

  const [framework, setFramework] = useState('vanilla');

  useEffect(() => {
    const arrayOfPath = window.location.pathname.split('/');
    const framework = arrayOfPath[arrayOfPath.length - 1];
    console.log(framework);

    setFramework(framework === 'frontend-introduction' ? 'vanilla' : framework);

  }, [window.location.pathname]);

  return (
    <Radio.Group className={styles.frameworkSwitcher} value={framework}>
      {FRAMEWORKS.map((framework) => (
        <Link
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
