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

const Container = styled.div`
  margin-top: 20px;
  display: flex;
  justify-content: center;

  & > div {
    cursor: pointer;
    margin: 0 15px;
  }
`

const Social = ({ path, title, iconSize }) => {
  const url = urljoin(config.siteUrl, config.pathPrefix, path);
  const fullTitle = `${config.siteTitle}: ${title}`
  return (
    <Container>
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
  iconSize: 40
}

export default Social;
