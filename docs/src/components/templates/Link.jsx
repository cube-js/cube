import React from 'react';
import Link from 'gatsby-link';

const StyledLink = props => <Link style={{textDecoration: 'none'}} {...props}>{props.children}</Link>;

export default StyledLink;