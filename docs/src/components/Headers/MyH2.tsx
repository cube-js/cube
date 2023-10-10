import React from 'react';

import { getHashFromContent } from './getHashFromContent';

const MyH2 = (props) => {
  const hash = getHashFromContent(props.children);
  return (<h2 name={hash} {...props} />);
}
export default MyH2;
