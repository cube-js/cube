import React from "react";
import { Link } from "gatsby";
import styled, { css } from 'styled-components'
import media from "styled-media-query";
import theme from '../../theme';
import githubLogo from '../Header/github-logo.svg'
import slackLogo from '../Header/slack-logo.svg'

const Container = styled.div`
  margin-top: 93px;
  min-width: 200px;
  margin-left: 30px;
  ${media.lessThan("small")`
    display: none;
  `}
`

const StickyContainer = styled.div`
  position: sticky;
  top: 50px;
`

const linkCss = css`
  color: ${theme.colors.darkPurple};
  font-weight: ${props => props.active ? "bold" : "normal"};
  text-decoration: none;
  margin-bottom: 20px;
  display: block;

  &:hover {
    text-decoration: underline;
  }
`

const StyledLink = styled(Link)`
  ${linkCss}
`

const ExternalLinksContainer = styled.div`
  border-top: 1px solid ${theme.colors.lightBlue};
  padding-top: 20px;
}
`

const ExternalLink = styled.a`
  ${linkCss}
  display: flex;
  align-items: center;
  color: ${theme.colors.grey};

  img {
    height: 20px;
    margin-right: 12px;
  }
`

const TableOfContents = ({ data, current, githubUrl }) => {
  return (
    <Container>
      <StickyContainer>
        {data.map(({ node }) => (
          <StyledLink
            active={current === node.fields.slug}
            to={node.fields.slug}
            key={node.frontmatter.title}
          >
            {node.frontmatter.title}
          </StyledLink>
        ))}
        <ExternalLinksContainer>
          <ExternalLink href={githubUrl}>
            <img alt="github" src={githubLogo} />
            Edit on Github
          </ExternalLink>
          <ExternalLink href="https://slack.cube.dev">
            <img alt="slack" src={slackLogo} />
            Ask Question in Slack
          </ExternalLink>
        </ExternalLinksContainer>
      </StickyContainer>
    </Container>
  )
};

TableOfContents.defaultProps = {
  data: []
}

export default TableOfContents;
