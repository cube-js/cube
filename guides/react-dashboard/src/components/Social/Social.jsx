import React from "react";
import urljoin from "url-join";
import {
  FacebookShareButton,
  LinkedinShareButton,
  TwitterShareButton,
  RedditShareButton,
  FacebookIcon,
  TwitterIcon,
  LinkedinIcon,
  RedditIcon
} from "react-share";
import styled from 'styled-components'
import config from "../../../data/SiteConfig";
import theme from '../../theme';
import media from "styled-media-query";

const Container = styled.div`
  margin-top: 20px;
  display: flex;
  justify-content: ${props => props.align};
  ${media.lessThan("medium")`
    justify-content: center;
  `}

  & > div {
    cursor: pointer;
    margin: 0 15px;
    &:first-child {
      margin-left: 0;
    }
  }
`

const Social = ({ path, title, iconSize, align }) => {
  const url = urljoin(config.siteUrl, config.pathPrefix, path);
  const fullTitle = [config.siteTitle, title].filter(v => !!v).join(": ")
  return (
    <Container align={align}>
      <RedditShareButton url={url} title={fullTitle}>
        <RedditIcon round size={iconSize} />
      </RedditShareButton>
      <FacebookShareButton url={url} title={fullTitle}>
        <FacebookIcon round size={iconSize} />
      </FacebookShareButton>
      <TwitterShareButton url={url} title={fullTitle}>
        <TwitterIcon round size={iconSize} />
      </TwitterShareButton>
    </Container>
  )
}

Social.defaultProps = {
  iconSize: 40,
  title: undefined,
  path: "",
  align: "center"
}

export default Social;
