import React from 'react';

import Header from './Desktop';
import MobileSearch from './MobileSearch';

type Props = {
  className: string;
  mobileSearch?: boolean;
};

const HeaderWrapper: React.FC<Props> = ({ mobileSearch = false, ...props }) =>
  mobileSearch ? <MobileSearch {...props} /> : <Header {...props} />;

export default HeaderWrapper;
