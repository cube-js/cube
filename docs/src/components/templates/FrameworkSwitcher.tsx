import React from 'react';
import { Radio } from 'antd';
import { RadioChangeEvent } from 'antd/lib/radio/interface';
import { navigate } from '@reach/router';
import {
  useFrameworkOfChoice,
  FRAMEWORKS,
} from '../../stores/frameworkOfChoice';

import * as styles from '../../../static/styles/index.module.scss';

type Props = {
  value: string;
};

const FrameworkSwitcher: React.FC<Props> = () => {
  const [frameworkOfChoice, setFrameworkOfChoice] = useFrameworkOfChoice();

  function onChange(event: RadioChangeEvent) {
    const framework = event.target.value;

    setFrameworkOfChoice(framework);
    navigate(
      `${process.env.PATH_PREFIX}/frontend-introduction${
        framework !== FRAMEWORKS[0].slug ? `/${framework}` : ''
      }`
    );
  }

  return (
    <Radio.Group
      className={styles.frameworkSwitcher}
      value={frameworkOfChoice}
      onChange={onChange}
    >
      {FRAMEWORKS.map((framework) => (
        <Radio.Button key={framework.slug} value={framework.slug}>
          {framework.name}
        </Radio.Button>
      ))}
    </Radio.Group>
  );
};

export default FrameworkSwitcher;
