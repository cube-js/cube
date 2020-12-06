import React from 'react';
import { Col } from 'antd';

import siderMobile from '../../pages/images/mobile-sider.svg';
import siderMobileInactive from '../../pages/images/mobile-sider-inactive.svg';
import close from '../../pages/images/close.svg';

import styles from '../../../static/styles/index.module.scss';
import { MobileModes } from '../../types';

type Props = {
  setMobileMode(props: any, mode?: MobileModes): void;
  mobileMode?: MobileModes;
};

const setMobileMode = (props: Props, mode: MobileModes) => {
  if (props.mobileMode !== 'content' && mode !== props.mobileMode) {
    return;
  }

  props.setMobileMode(props.mobileMode === mode ? 'content' : mode);
};

const defaultProps: Partial<Props> = {
  mobileMode: MobileModes.CONTENT,
};

const MobileFooter: React.FC<Props> = (props) => {
  const mergedProps = { ...defaultProps, ...props };

  let mobileSiderIcon;

  switch (props.mobileMode) {
    case 'menu':
      mobileSiderIcon = close;
      break;
    case 'search':
      mobileSiderIcon = siderMobileInactive;
      break;
    default:
      mobileSiderIcon = siderMobile;
  }

  return (
    <div className={styles.mobileFooter}>
      <Col md={0} xs={24}>
        <div
          className={styles.mobileFooterButton}
          onTouchStart={(e) => {
            e.preventDefault();
            setMobileMode(mergedProps, MobileModes.MENU);
          }}
          onClick={() => setMobileMode(mergedProps, MobileModes.MENU)}
          onTouchMove={(e) => e.preventDefault()}
        >
          <img
            src={mobileSiderIcon}
            alt=""
            className={styles.mobileFooterImage}
          />
        </div>
      </Col>
    </div>
  );
};

export default MobileFooter;
