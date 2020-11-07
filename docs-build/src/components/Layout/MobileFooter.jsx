import React from 'react';
import { Col } from 'antd';
import PropTypes from 'prop-types';

import siderMobile from '../../pages/images/mobile-sider.svg';
//import searchMobile from '../pages/images/mobile-search.svg';
//import searchMobileInactive from '../pages/images/mobile-search-inactive.svg';
import siderMobileInactive from '../../pages/images/mobile-sider-inactive.svg';
import close from '../../pages/images/close.svg';

import styles from '../../../static/styles/index.module.scss';

const setMobileMode = (props, mode) => {
  if (props.mobileMode !== 'content' && mode !== props.mobileMode) {
    return;
  }

  props.setMobileMode(props.mobileMode === mode ? 'content' : mode);
};

const MobileFooter = props => {
  let mobileSiderIcon;
  //let mobileSearchIcon;

  switch (props.mobileMode) {
    case 'menu':
      mobileSiderIcon = close;
      //mobileSearchIcon = searchMobileInactive;
      break;
    case 'search':
      //mobileSearchIcon = close;
      mobileSiderIcon = siderMobileInactive;
      break;
    default:
      //mobileSearchIcon = searchMobile;
      mobileSiderIcon = siderMobile;
  }

  return (
    <div className={styles.mobileFooter}>
      <Col
        md={0}
        xs={24}
      >
        <div
          className={styles.mobileFooterButton}
          onTouchStart={e => e.preventDefault() && setMobileMode(props, 'menu')}
          onClick={() => setMobileMode(props, 'menu')}
          onTouchMove={e => e.preventDefault()}
        >
          <img src={mobileSiderIcon} alt="" className={styles.mobileFooterImage}/>
        </div>
      </Col>
    {
      //<Col
      //  md={0}
      //  xs={12}
      //>
      //  <div
      //    className={cx(styles.mobileFooterButton, styles.mobileFooterSearch)}
      //    onTouchStart={e => e.preventDefault() && setMobileMode(props, 'search')}
      //    onClick={() => setMobileMode(props, 'search')}
      //    onTouchMove={e => e.preventDefault()}
      //  >
      //    <img src={mobileSearchIcon} alt="" className={cx(styles.mobileFooterImage, styles.mobileFooterSearch)} />
      //  </div>
      //</Col>
    }
    </div>
  )
}

MobileFooter.propTypes = {
  setMobileMode: PropTypes.func.isRequired,
  mobileMode: PropTypes.oneOf(['content', 'menu', 'search']),
}

MobileFooter.defaultProps = {
  mobileMode: 'content'
}

export default MobileFooter;
