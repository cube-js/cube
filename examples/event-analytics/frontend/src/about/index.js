import React from 'react';

import { withStyles } from '@material-ui/core/styles';
import WindowTitle from '../components/WindowTitle';
import PrismCode from '../components/PrismCode';

import Grid from '@material-ui/core/Grid';
import Card from '@material-ui/core/Card';
import CardHeader from '@material-ui/core/CardHeader';
import CardMedia from '@material-ui/core/CardMedia';
import CardContent from '@material-ui/core/CardContent';
import Typography from '@material-ui/core/Typography';

import schemaImg from './schema.png';

const styles = theme => ({
  withMinHeight: {
    minHeight: 600,
    [theme.breakpoints.down('sm')]: {
     minHeight: 0
    }
  },
  twoColumn: {
    gridColumnEnd: "span 2"
  },
  withBottomMargin: {
    marginBottom: 20
  }
});

const trackingCode =  `<script type="text/javascript">
  ;(function(p,l,o,w,i,n,g){if(!p[i]){p.GlobalSnowplowNamespace=p.GlobalSnowplowNamespace||[];
  p.GlobalSnowplowNamespace.push(i);p[i]=function(){(p[i].q=p[i].q||[]).push(arguments)
  };p[i].q=p[i].q||[];n=l.createElement(o);g=l.getElementsByTagName(o)[0];n.async=1;
  n.src=w;g.parentNode.insertBefore(n,g)}}(window,document,"script","//d1fc8wv8zag5ca.cloudfront.net/2.10.2/sp.js","snowplow"));

  window.snowplow('newTracker', 'cf', '%REACT_APP_CLOUD_FRONT_ID%.cloudfront.net', { post: false });
</script>
`

const createAthenaTable = `CREATE EXTERNAL TABLE IF NOT EXISTS default.cloudfront_logs (
  \`date\` DATE,
  time STRING,
  location STRING,
  bytes BIGINT,
  requestip STRING,
  method STRING,
  host STRING,
  uri STRING,
  status INT,
  referrer STRING,
  useragent STRING,
  querystring STRING,
  cookie STRING,
  resulttype STRING,
  requestid STRING,
  hostheader STRING,
  requestprotocol STRING,
  requestbytes BIGINT,
  timetaken FLOAT,
  xforwardedfor STRING,
  sslprotocol STRING,
  sslcipher STRING,
  responseresulttype STRING,
  httpversion STRING,
  filestatus STRING,
  encryptedfields INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 's3://CloudFront_bucket_name/AWSLogs/Account_ID/'
TBLPROPERTIES ( 'skip.header.line.count'='2' )
`

const cubejsSchema = `cube(\`Events\`, {
  sql:
    \`SELECT
      from_iso8601_timestamp(to_iso8601(date) || 'T' || "time") as time,
      url_decode(url_decode(regexp_extract(querystring, 'e', 1))) as event,
      url_decode(url_decode(regexp_extract(querystring, 'page', 1))) as page_title,
    FROM cloudfront_logs
    \`,

  measures: {
    count: {
      type: \`count\`
    },

    pageView: {
      type: \`count\`,
      filters: [
        { sql: \`\${CUBE}.event = 'pv'\` }
      ]
    },
  },

  dimensions: {
    pageTitle: {
      sql: \`page_title\`,
      type: \`string\`
    }
  }
});`

const AboutPage = ({ classes }) => (
  <>
    <WindowTitle title="About" />
    <Grid container spacing={24}>
      <Grid item xs={12} md={6}>
        <Card className={classes.withMinHeight}>
          <CardHeader title="Architecture" />
          <CardMedia component="img" image={schemaImg} />
        </Card>
      </Grid>
      <Grid item xs={12} md={6}>
        <Card className={classes.withMinHeight}>
          <CardHeader title="Installation" />
          <CardContent>
            <Typography variant="h6">
              Setup Snowplow Cloudfront Collector
            </Typography>
            <Typography variant="body1" className={classes.withBottomMargin}>
    You need to upload a tracking pixel to Amazon CloudFront CDN. The Snowplow Tracker sends data to the collector by making a GET request for the pixel and passing data as a query string parameter. The CloudFront Collector uses CloudFront logging to record the request (including the query string) to an S3 bucket.
            </Typography>
            <Typography variant="h6">
              Install Javascript Tracker
            </Typography>
            <Typography variant="body1" className={classes.withBottomMargin}>
              Snowplow Javascript Tracker is similar to Google Analytics’s tracking code or Mixpanel’s, so you need to just embed it into your HTML page. See snippet example below.
            </Typography>
            <Typography variant="h6">
              Create Athena Table
            </Typography>
            <Typography variant="body1" className={classes.withBottomMargin}>
              Once you have the data, which is CloudFront logs, in the S3 bucket, you can query it with Athena. All you need to do is create a table for CloudFront logs. See SQL code below.
            </Typography>
            <Typography variant="h6">
              Setup Cube.js Schema
            </Typography>
            <Typography variant="body1" className={classes.withBottomMargin}>
              Cube.js uses Data Schema to generate and execute SQL. You can express all required transformation in the schema, it also could be generated dynamically. See code below for an example or <a href="https://cube.dev/docs/getting-started-cubejs-schema">learn more about it here</a>
            </Typography>
          </CardContent>
        </Card>
      </Grid>
      <Grid item xs={12}>
        <Card >
          <CardHeader title="Tracking Code" />
          <CardContent>
            <PrismCode code={trackingCode} />
          </CardContent>
        </Card>
      </Grid>
      <Grid item xs={12} sm={6}>
        <Card>
          <CardHeader title="Create Athena table" />
          <CardContent>
            <PrismCode code={createAthenaTable} />
          </CardContent>
        </Card>
      </Grid>
      <Grid item xs={12} sm={6}>
        <Card>
          <CardHeader title="Setup Cube.js Schema" />
          <CardContent>
            <PrismCode code={cubejsSchema} />
          </CardContent>
        </Card>
      </Grid>
      <Grid item xs={12}>
        <Card>
          <CardHeader title="Visualize Results" />
          <CardContent>
              <iframe title="Results" src="https://codesandbox.io/embed/pkj4pk0x1j?fontsize=12&hidenavigation=1" style={{width: "100%", height: 1000, border: 0, borderRadius: "4px", overflow: "hidden"}} sandbox="allow-modals allow-forms allow-popups allow-scripts allow-same-origin"></iframe>
          </CardContent>
        </Card>
      </Grid>
    </Grid>
  </>
);

export default withStyles(styles)(AboutPage);
