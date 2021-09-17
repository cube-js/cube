import React from 'react';
import kebabCase from 'lodash/kebabCase';

const MyH2 = (props) => <h2 name={kebabCase(props.children)} {...props} />;
export default MyH2;
