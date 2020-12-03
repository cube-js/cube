import React from 'react';
import { Radio } from 'antd';
import { navigate } from '@reach/router';
import { useFrameworkOfChoice, FRAMEWORKS } from '../../stores/frameworkOfChoice';

import styles from '../../../static/styles/index.module.scss';

const FrameworkSwitcher = () => {
  const [frameworkOfChoice, setFrameworkOfChoice] = useFrameworkOfChoice();

  function onChange(event) {
    const framework = event.target.value;

    setFrameworkOfChoice(framework);
    navigate(`${__PATH_PREFIX__}/frontend-introduction${framework !== FRAMEWORKS[0].slug ? `/${framework}` : ''}`);
  }

  return <Radio.Group className={styles.frameworkSwitcher} value={frameworkOfChoice} onChange={onChange}>
    { FRAMEWORKS.map(framework => (
      <Radio.Button key={framework.slug} value={framework.slug}>
        { framework.name }
      </Radio.Button>
    ))}
  </Radio.Group>
};

export default FrameworkSwitcher;
