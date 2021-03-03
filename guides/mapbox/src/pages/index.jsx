import React from 'react';
import Helmet from 'react-helmet';
import { graphql } from 'gatsby';
import { Header, Hero, Footer, Feature, Social, SEO, PartsListing, Layout } from 'guides-base';
import config from '../../data/SiteConfig';

import featureOneImg from './feature-1.svg';
import featureTwoImg from './feature-2.svg';
import featureThreeImg from './feature-3.svg';

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
          title="JavaScript Map Data Visualization with Mapbox"
          subtitle="Learn how to build a map-based data visualization with Mapbox, Cube.js, and React"
          demoUrl="https://mapbox-demo.cube.dev"
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
          metaTitle="MAP-BASED DATA VISUALIZATION"
          title="Learn How to Visualize Geospatial Data"
          text="Explore how to work with GeoJSON-encoded locations and create comprehensible and graphic map-based data visualizations."
        />
        <Feature
          imageAlign="right"
          image={featureTwoImg}
          metaTitle="CUBE.JS FOR API"
          title="Bootstrap an API for Your App in Minutes"
          text="Learn how to create an API with Cube.js, a powerful open-source analytical API platform, and define a data schema for your dataset."
        />
        <Feature
          imageAlign="left"
          image={featureThreeImg}
          metaTitle="MAPBOX FOR DATAVIZ"
          title="Learn How to Create Heatmaps, Point Density Maps, and Choropleth Maps"
          text="Explore how to work with different map primitives provided by Mapbox, a very popular set of tools for working with maps, navigation, and location-based search, etc."
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
