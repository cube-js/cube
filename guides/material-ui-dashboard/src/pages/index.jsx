import React from 'react';
import Helmet from 'react-helmet';
import { graphql } from 'gatsby';
import { Header, Hero, Footer, Feature, Social, SEO, PartsListing, Layout } from 'guides-base';
import config from '../../data/SiteConfig';

import featureOneImg from './feature-1.svg';
import featureTwoImg from './feature-2.svg';
import featureThreeImg from './feature-3.svg';

import styled from 'styled-components';
import media from 'styled-media-query';

import hero from './hero.png';

const StyledHeroImage = styled.img`
  ${media.greaterThan('large')`
    margin-top: -50px;
  `}
`;

class Index extends React.Component {
  render() {
    const partsEdges = this.props.data.allMarkdownRemark.edges;
    return (
      <Layout config={config}>
        <SEO config={config} />
        <Helmet title={config.siteTitle} />
        <Header githubUrl={config.githubUrl} />
        <Hero
          title="Building Material UI Dashboard with Cube.js"
          subtitle="Learn how to build Material UI Dashboard with Cube.js."
          demoUrl="https://material-ui-dashboard.cubecloudapp.dev/"
          startUrl={partsEdges[0].node.fields.slug}
          socialButtons={<Social align="flex-start" siteTitle={config.siteTitle} siteUrl={config.siteUrl} />}
          media={
            <video muted autoPlay playsInline loop preload="auto" poster={hero}>
              <source type="video/mp4" src="video/preview.mp4" />
            </video>
          }
          withFrame
        />
        <Feature
          imageAlign="left"
          image={featureOneImg}
          metaTitle="Clear"
          title="Step by step"
          text="Learn how to create your own, React Material UI dashboard. You will learn step by step how to create dashboard with queries to your database without writing SQL code."
        />
        <Feature
          imageAlign="right"
          image={featureTwoImg}
          metaTitle="Visualisation"
          title="Easy to create"
          text="This guide shows you how to build different types of graphs and how to display any data. This guide includes a Bar chart, Doughnut chart, KPI cards, and Datatable. Using them as an example or using the chart renderer component, you can develop your dashboard using any chart/data display."
        />
        <Feature
          imageAlign="left"
          image={featureThreeImg}
          metaTitle="Performance first"
          title="Fast and Scalable"
          text="The response time is under 50 ms by using Cube.js pre-aggregations. It scales well for tracking up to several million daily active users. To achieve this performance, Cube.js stores and manages aggregated tables in MySQL with a 5-minute refresh rate."
        />
        <PartsListing partsEdges={partsEdges} />
        <Footer />
      </Layout>
    );
  }
}

export default Index;

/* eslint no-undef: "off" */
export const pageQuery = graphql`
  query IndexQuery {
    allMarkdownRemark(limit: 2000, sort: { fields: [frontmatter___order], order: ASC }) {
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
