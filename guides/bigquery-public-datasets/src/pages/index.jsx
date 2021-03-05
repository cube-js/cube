import React from 'react';
import Helmet from 'react-helmet';
import { graphql } from 'gatsby';
import { Header, Hero, Footer, Feature, Social, SEO, PartsListing, Layout } from 'guides-base';
import config from '../../data/SiteConfig';

import featureOneImg from './dtip1f1mcth7svxvv7v2.png';
import featureTwoImg from './5rdos5okrrindw23m5di.png';
import featureThreeImg from './amefzcyiqamzfleopqw8.png';

import hero from './demo.png';

class Index extends React.Component {
  render() {
    const partsEdges = this.props.data.allMarkdownRemark.edges;
    return (
      <Layout config={config}>
        <SEO config={config} />
        <Helmet title={config.siteTitle} />
        <Header githubUrl={config.githubUrl} />
        <Hero
          title="BigQuery Public Datasets for COVID-19 Impact Research"
          subtitle="We'll explore how to build an analytical application on top of Google BigQuery, a serverless data warehouse, and use a few public datasets to visualize the impact of the COVID-19 pandemic on people's lives."
          demoUrl="https://bigquery-public-datasets-demo.cube.dev"
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
          metaTitle="SERVERLESS DATA WAREHOUSE"
          title="Build Your Own BigQuery App"
          text="Explore how to create an analytical app that takes data from BigQuery, a serverless data warehouse."
        />
        <Feature
          imageAlign="right"
          image={featureTwoImg}
          metaTitle="CUBE.JS ANALYTICAL API"
          title="Create an Analytical API in Minutes"
          text="Learn how to use BigQuery with Cube.js, a powerful open-source analytical API platform, to bootstrap an analytical API and define a data schema for your dataset."
        />
        <Feature
          imageAlign="left"
          image={featureThreeImg}
          metaTitle="COVID-19 PUBLIC DATASETS"
          title="Research the Impact of COVID-19"
          text="Learn all about public datasets in BigQuery and put them to work in your own analytical app. Get insights about the impact of COVID-19."
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
