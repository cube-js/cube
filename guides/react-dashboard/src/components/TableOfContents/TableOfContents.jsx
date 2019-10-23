import React from "react";
import { Link, StaticQuery, graphql } from "gatsby";
import styled, { css } from 'styled-components'
import media from "styled-media-query";
import theme from '../../theme';
import githubLogo from '../Header/github-logo.svg'
import slackLogo from '../Header/slack-logo.svg'

/* eslint no-undef: "off" */
const query = graphql`
  query TableOfContentsQuery {
    allMarkdownRemark(
      limit: 2000
      sort: { fields: [frontmatter___order], order: ASC }
    ) {
      edges {
        node {
          fields {
            slug
          }
          frontmatter {
            title
            order
          }
        }
      }
    }
  }
`;

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

const TableOfContents = ({ current }) => (
  <StaticQuery
    query={query}
    render={data => (
      <Container>
        <StickyContainer>
          {data.allMarkdownRemark.edges.map(({ node }) => (
            <StyledLink
              active={current === node.fields.slug}
              to={node.fields.slug}
              key={node.frontmatter.title}
            >
              {node.frontmatter.title}
            </StyledLink>
          ))}
          <ExternalLinksContainer>
            <ExternalLink href="https://github.com/cube-js/cube.js">
              <img src={githubLogo} />
              Edit on Github
            </ExternalLink>
            <ExternalLink href="https://slack.cube.dev">
              <img src={slackLogo} />
              Ask Question in Slack
            </ExternalLink>
          </ExternalLinksContainer>
        </StickyContainer>
      </Container>
    )}
  />
);

export default TableOfContents;
