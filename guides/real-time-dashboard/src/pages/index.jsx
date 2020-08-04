import React from "react";
import Helmet from "react-helmet";
import { graphql } from "gatsby";
import { Header, Hero, Footer, Feature, Social, SEO, PartsListing, Layout } from "guides-base";
import config from "../../data/SiteConfig";

import featureOneImg from "./feature-1.png";
import featureOneTwo from "./feature-2.png";
import featureOneThree from "./feature-3.png";

import hero from "./hero.png";

class Index extends React.Component {
  render() {
    const partsEdges = this.props.data.allMarkdownRemark.edges;
    return (
      <Layout config={config}>
        <SEO config={config} />
        <Helmet title={config.siteTitle} />
        <Header githubUrl={config.githubUrl} />
        <Hero
          title="Real-Time Dashboard Guide"
          subtitle="Learn how to build a real-time dashboard with open-source tools."
          demoUrl="https://real-time-dashboard-demo.cube.dev/"
          startUrl={partsEdges[0].node.fields.slug}
          socialButtons={<Social align="flex-start" siteTitle={config.siteTitle} siteUrl={config.siteUrl} />}
          media={ <img alt="hero" src={hero} /> }
        />
        <Feature
          imageAlign='left'
          image={featureOneImg}
          metaTitle="tools"
          title="Open-Source Tools"
          text="This guide shows how to build a full-stack real-time dashboard with only open-source tools—from the database to the visualizations. You’ll learn how to set up a database, seed it with data, build an API endpoint on top of it, and then load and update charts on the frontend via WebSockets in real time."
        />
        <Feature
          imageAlign='right'
          image={featureOneTwo}
          metaTitle="databases"
          title="Real-Time Dashboard with MongoDB"
          text="You will learn how to build a real-time dashboard with React, Cube.js, and MongoDB. Even though MongoDB is a NoSQL database, it recently released MongoDB Connector for BI, which allows using the full power of SQL to build an analytics dashboard on top of data in MongoDB. We will show how to analyze the stream of events and update the dashboard in real time."
        />
        <Feature
          imageAlign='left'
          image={featureOneThree}
          metaTitle="databases"
          title="Real-Time Dashboard with BigQuery"
          text="BigQuery is a great database for the analytic workload. It dramatically outperforms traditional RDBMS in processing large datasets. But it has its caveats, which can affect both performance and pricing. You will learn how to manage requests sent to BigQuery to control the cost, but not give up on the performance."
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
