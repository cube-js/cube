import React from 'react';
import Link from 'gatsby-link';

type Props = {
  to: string;
};

const StyledLink: React.FC<Props> = (props) => (
  <Link style={{ textDecoration: 'none' }} {...props}>
    {props.children}
  </Link>
);

export default StyledLink;
