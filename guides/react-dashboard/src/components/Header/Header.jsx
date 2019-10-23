import React from "react";
import { Link } from "gatsby";
import styled from 'styled-components';
import media from "styled-media-query";
import theme from '../../theme';
import logo from './logo.svg';
import githubLogo from './github-logo.svg'
import slackLogo from './slack-logo.svg'

const Container = styled.div`
  padding: 54px ${theme.landingPadding} 50px ${theme.landingPadding};
  ${media.lessThan("large")`
    padding-left: ${theme.landingPaddingLessThenLarge};
    padding-right: ${theme.landingPaddingLessThenLarge};
  `}
  background-color: ${theme.colors.lightBlue};
  display: flex;
  justify-content: space-between;
  align-items: center;
`;

const StyledLink = styled.a`
  ${media.lessThan("large")`
    margin-left: 20px;
  `}
  margin-left: 60px;
  color: ${theme.colors.purple};
  text-decoration: none;
  span {
    vertical-align: super;
    margin-left: 15px;
    ${media.lessThan("medium")`
      display: none;
    `}
    ${media.lessThan("large")`
      font-size: 13px;
    `}
  }
  &:hover, &:active {
    color: ${theme.colors.darkPurple};
  }
`

const LogoLink = styled(Link)`
  display: flex;
  align-items: center;
  font-size: 32px;
  color: ${theme.colors.grey};
  text-decoration: none;
  ${media.lessThan("small")`
    font-size: 20px;
  `}

  img {
    margin-right: 17px;

    ${media.lessThan("small")`
      max-width: 120px;
      margin-right: 10px;
    `}
  }
`

const Header = () => (
  <Container>
    <LogoLink to="/">
      <img src={logo} alt="" />
      learn
    </LogoLink>
    <div>
      <StyledLink href="https://github.com/cube-js/cube.js">
        <img src={githubLogo} alt="" />
        <span>Check the Code on Github</span>
      </StyledLink>
      <StyledLink href="https://slack.cube.dev">
        <img src={slackLogo} alt="" />
        <span>Join Discussion in Slack</span>
      </StyledLink>
    </div>
  </Container>
)

export default Header;
