import React from 'react';
import { Col } from 'antd';

import siderMobile from '../../pages/images/mobile-sider.svg';
import siderMobileInactive from '../../pages/images/mobile-sider-inactive.svg';
import close from '../../pages/images/close.svg';

import * as styles from '../../../static/styles/index.module.scss';
import { MobileModes } from '../../types';

type Props = {
  setMobileMode(mode: MobileModes): void;
  mobileMode?: MobileModes;
};

const setMobileMode = (props: Props, mode: MobileModes) => {
  const nextMode = mode === MobileModes.MENU ? MobileModes.CONTENT : mode;
  props.setMobileMode(
    props.mobileMode === nextMode ? MobileModes.MENU : nextMode
  );
};

const defaultProps: Partial<Props> = {
  mobileMode: MobileModes.CONTENT,
};

const MobileFooter: React.FC<Props> = (props) => {
  const mergedProps = { ...defaultProps, ...props };

  let mobileSiderIcon;

  switch (props.mobileMode) {
    case MobileModes.MENU:
      mobileSiderIcon = close;
      break;
    case MobileModes.SEARCH:
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
          onClick={() => setMobileMode(mergedProps, MobileModes.MENU)}
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
