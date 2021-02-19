import React from 'react';
import Helmet from 'react-helmet';
import { graphql } from 'gatsby';
import { Header, Hero, Footer, Feature, Social, SEO, PartsListing, Layout } from 'guides-base';
import config from '../../data/SiteConfig';

import featureOneImg from './o7ymw2ol9u7kxg3vjjz9.png';
import featureTwoImg from './yjneoih6qbkp5kmhyayf.png';
import featureThreeImg from './a05i9ntjdm0biqqqpw4a.png';

import hero from './demo.gif';

class Index extends React.Component {
  render() {
    const partsEdges = this.props.data.allMarkdownRemark.edges;
    return (
      <Layout config={config}>
        <SEO config={config} />
        <Helmet title={config.siteTitle} />
        <Header githubUrl={config.githubUrl} />
        <Hero
          title="ClickHouse Dashboard: Analytics Tutorial"
          subtitle="We'll explore how to create a dashboard on top of ClickHouse, a fast open-source analytical database. We'll build a stock market data visualization with candlestick charts, learn the impact of WallStreetBets, and observe how fast ClickHouse works."
          demoUrl="https://clickhouse-dashboard-demo.cube.dev"
          startUrl={partsEdges[0].node.fields.slug}
          socialButtons={<Social align="flex-start" siteTitle={config.siteTitle} siteUrl={config.siteUrl} />}
          media={
            <img src={hero} />
          }
          withFrame
        />
        <Feature
          imageAlign="left"
          image={featureOneImg}
          metaTitle="FAST OPEN-SOURCE ANALYTICAL DATABASE"
          title="Build Your Own ClickHouse Dashboard"
          text="Explore how to create a dashboard that takes data from ClickHouse, a fast open-source column-oriented database."
        />
        <Feature
          imageAlign="right"
          image={featureTwoImg}
          metaTitle="CUBE.JS ANALYTICAL API"
          title="Create an Analytical API in Minutes"
          text="Learn how to use ClickHouse with Cube.js, a powerful open-source analytical API platform, to bootstrap an analytical API and define a data schema for your dataset."
        />
        <Feature
          imageAlign="left"
          image={featureThreeImg}
          metaTitle="STOCK MARKET DATA"
          title="Use Your Dashboard to Explore Stock Prices"
          text="Learn all about candlestick charts, stock market prices, and the impact of WallStreetBets. Building on that example, observe how fast ClickHouse and Cube.js work."
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
