import React from "react";
import styled from 'styled-components';
import media from "styled-media-query";
import { Link } from "gatsby";
import theme from '../../theme';

const Container = styled.div`
  border-top: 1px solid rgba(161, 161, 181, 0.3);
`

const InnerContainer = styled.div`
  max-width: ${theme.contentPageMaxWidth};
  padding: 0 30px;
  margin: 50px auto;
  display: flex;
  justify-content: space-between;
  ${media.lessThan("small")`
    flex-direction: column;
  `}
`;

const StyledLink = styled(Link)`
  color: ${theme.colors.darkPurple};
  text-decoration: none;
  font-size: 20px;
  margin-left: ${props => props.right ? "auto" : "0"};
  ${media.lessThan("small")`
    font-size: 16px;
    &:first-child {
      margin-bottom: 10px;
    }
  `}
`

const NextPrev = ({
  prevSlug,
  prevTitle,
  nextSlug,
  nextTitle
}) => (
  <Container>
    <InnerContainer>
      { prevSlug && (
        <StyledLink to={prevSlug}>
          {`←  ${prevTitle}`}
        </StyledLink>
      )}
      { nextSlug && (
        <StyledLink to={nextSlug} right>
          {`${nextTitle}  →`}
        </StyledLink>
      )}
    </InnerContainer>
  </Container>
);

export default NextPrev;
