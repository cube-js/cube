import React from 'react';
import PropTypes from 'prop-types';

import Header from './Desktop';
import MobileSearch from './MobileSearch';

const HeaderWrapper = props => (
  props.mobileSearch ? <MobileSearch {...props} /> : <Header {...props} />
)

HeaderWrapper.propTypes = {
  mobileSearch: PropTypes.bool
}

HeaderWrapper.defaultProps = {
  mobileSearch: false
}

export default HeaderWrapper;
