import React from "react";
import styled from 'styled-components';
import { Link } from "gatsby";
import media from "styled-media-query";
import theme, { sharedStyles } from '../../theme';
import browserControls from './browser-controls.svg';

const Container = styled.div`
  background-color: ${theme.colors.lightBlue};
  padding: 0 ${theme.landingPadding};
  padding-bottom: 180px;
  padding-top: 110px;
  ${media.lessThan("large")`
    padding-left: ${theme.landingPaddingLessThenLarge};
    padding-right: ${theme.landingPaddingLessThenLarge};
  `}
  ${media.lessThan("medium")`
    padding-top: 20px;
    padding-bottom: 40px;
  `}
`

const InnerContainer = styled.div`
  max-width: 1261px;
  margin: 0 auto;
  display: flex;
  justify-content: space-between;
  ${media.lessThan("medium")`
    justify-content: center;
    flex-direction: column;
    text-align: center;
  `}
`

const CopyContainer = styled.div`
  max-width: 510px;
  ${media.lessThan("medium")`
    align-self: center;
    margin-bottom: 40px;
  `}

`

const Title = styled.h1`
  color: ${theme.colors.darkPurple};
  font-size: 42px;
  margin-bottom: 30px;
  margin-top: 0;
`

const Subtitle = styled.div`
  color: ${theme.colors.grey};
  font-size: 20px;
  line-height: 30px;
`

const MediaContainer = styled.div`
  background-image: url(${browserControls});
  background-position: 11px 6px;
  background-repeat: no-repeat;
  background-color: white;
  box-shadow: 1px 1px 4px 0 rgba(0, 0, 0, 0.15);
  padding: 5px;
  border-radius: 10px;
  padding-top: 20px;
  padding-bottom: 2px;
  ${media.greaterThan("medium")`
    margin-left: 15px;
  `}
  img, video {
    max-width: 630px;
    width: 100%;
    border: 10px solid #F3F3FB;
    border-radius: 10px;
    box-sizing: border-box;
  }
  ${media.lessThan("large")`
    & > img { max-width: 400px; }
  `}
  ${media.lessThan("medium")`
    align-self: center;
    & > img { max-width: 100%; }
  `}
`

const PrimaryButton = styled(Link)`
  ${sharedStyles.button};

  color: white;
  background-color: ${theme.colors.red};

  &:hover, &:focus {
    background-color: #FB3972;
  }
`

const SecondaryButton = styled.a`
  ${sharedStyles.button};

  color: ${theme.colors.red};
  border: 1px solid ${theme.colors.red};
  border-radius: 4px;
  text-align: center;
  min-width: 240px;
  box-sizing: border-box;

  &:hover, &:focus {
    background: linear-gradient(0deg, #FB3972, #FB3972), #FF6492;
    color: white;
  }
`

const ButtonsContainer = styled.div`
  display: flex;
  margin-top: 45px;
  margin-bottom: 25px;
  justify-content: space-between;
  flex-wrap: wrap;
  ${media.lessThan("small")`
    flex-direction: column;
    & > :first-child {
      margin-bottom: 20px;
    }
  `}
`

const Hero = ({
  startUrl,
  socialButtons,
  media,
  title,
  subtitle,
  demoUrl
}) => (
  <Container>
    <InnerContainer>
      <CopyContainer>
        <Title> { title }</Title>
        <Subtitle> { subtitle } </Subtitle>
        <ButtonsContainer>
          <PrimaryButton to={startUrl}>Start Learning</PrimaryButton>
          <SecondaryButton href={demoUrl}>Demo</SecondaryButton>
        </ButtonsContainer>
        { socialButtons }
      </CopyContainer>
      <MediaContainer>
        { media }
      </MediaContainer>
    </InnerContainer>
  </Container>
);

export default Hero;
