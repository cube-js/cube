import React from 'react';
import Helmet from 'react-helmet';
import { graphql } from 'gatsby';
import { Header, Hero, Footer, Feature, Social, SEO, PartsListing, Layout } from 'guides-base';
import config from '../../data/SiteConfig';

import featureOneImg from './lpelpjdemi8bc9nk2ma1.png';
import featureTwoImg from './screenshot.png';
import featureThreeImg from './screenshot2.png';

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
          title="Multi-Tenant Analytics with Auth0 and Cube.js"
          subtitle="We'll learn how to secure web applications with industry-standard and proven authentication mechanisms such as JSON Web Tokens, JSON Web Keys, OAuth 2.0 protocol."
          demoUrl="https://multi-tenant-analytics-demo.cube.dev"
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
          metaTitle="AUTHENTICATION & AUTHORIZATION"
          title="Secure Your Analytical App"
          text="Start with an openly accessible, insecure analytical app and walk through a series of steps to turn it into a secure app integrated with an external authentication provider."
        />
        <Feature
          imageAlign="right"
          image={featureTwoImg}
          metaTitle="MULTI-TENANCY VIA SECURITY CONTEXT"
          title="Serve Data to Multiple Tenants"
          text="Build a multi-tenant analytical app with role-based access control based on security claims which are stored in JSON Web Tokens"
        />
        <Feature
          imageAlign="left"
          image={featureThreeImg}
          metaTitle="EXTERNAL AUTHENTICATION PROVIDER"
          title="Integrate with Auth0"
          text="Add integration with an external provider such as Auth0 and use JSON Web Key Sets to validate JSON Web Tokens"
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
