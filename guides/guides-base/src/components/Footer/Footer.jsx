import React from "react";
import styled from 'styled-components';
import theme from '../../theme';
import media from "styled-media-query";
import logo from './logo.svg';
import slack from './slack-icon.svg';
import twitter from './twitter-icon.svg';
import github from './github-icon.svg';

const Container = styled.footer`
  padding: 70px ${theme.landingPadding};
  ${media.lessThan("large")`
    padding: 70px ${theme.landingPaddingLessThenLarge};
  `}
  ${media.lessThan("small")`
    font-size: 14px;
    flex-direction: column;
    padding-top: 20px;
    padding-bottom: 20px;
    & > :first-child {
      margin-bottom: 20px;
    }
  `}
  background-color: ${theme.colors.purple};
  display: flex;
  justify-content: space-between;
  color: white;
`;

const ItemsContainer = styled.div`
  display: flex;
  align-items: center;
`

const Social = styled(ItemsContainer)`
  & a {
    margin-left: 20px;
  }
  ${media.greaterThan("small")`
    & span {
      margin-left: 22px;
    }

  `}
`

const Logo = styled(ItemsContainer)`
  & > a {
    margin-left: 20px;
  }
`;

const Footer = () => (
  <Container>
    <Logo>
      Created by
      <a href="https://cube.dev">
        <img alt="logo" src={logo} />
      </a>
    </Logo>
    <Social>
      Get in Touch
      <span>
        <a href="https://slack.cube.dev">
          <img alt="slack" src={slack} />
        </a>
        <a href="https://twitter.com/thecubejs">
          <img alt="twitter" src={twitter} />
        </a>
        <a href="https://github.com/cube-js/cube.js">
          <img alt="github" src={github} />
        </a>
      </span>
    </Social>
  </Container>
)

export default Footer;
