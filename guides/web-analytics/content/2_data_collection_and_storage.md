---
order: 2
title: "Data Collection and Storage"
---

We're going to use Snowplow for data collection, S3 for storage, and Athena to query the data in S3.

## Data Collection with Snowplow

Snowplow is an analytics platform to collect, enrich, and store data. We'll use the Snowplow Javascript tracker on our website, which generates event-data and sends it to the Snowplow Collector to load to S3.

Before loading the data, we'll use Enricher to turn IP addresses into coordinates. We'll use AWS Kinesis to manage data streams for collection, enrichment, and then finally loading into S3. The schema below illustrates the whole process.

![](/images/2-schema-1.png)

Let's start by setting up the tracker. Adding Snowplow's tracker to the website is the same, as adding Google Analytics or Mixpanel tracker. You need to add the asynchronous Javascript code, which loads the tracker itself.

```javascript
<!-- Snowplow starts plowing -->
<script type="text/javascript">
;(function(p,l,o,w,i,n,g){if(!p[i]){p.GlobalSnowplowNamespace=p.GlobalSnowplowNamespace||[];
p.GlobalSnowplowNamespace.push(i);p[i]=function(){(p[i].q=p[i].q||[]).push(arguments)
};p[i].q=p[i].q||[];n=l.createElement(o);g=l.getElementsByTagName(o)[0];n.async=1;
n.src=w;g.parentNode.insertBefore(n,g)}}(window,document,"script","//d1fc8wv8zag5ca.cloudfront.net/2.10.2/sp.js","snowplow"));

window.snowplow('newTracker', 'cf', '{{MY-COLLECTOR-URI}}', { // Initialise a tracker
  appId: '{{MY-SITE-ID}}',
  cookieDomain: '{{MY-COOKIE-DOMAIN}}'
});

window.snowplow('trackPageView');
</script>
<!-- Snowplow stops plowing -->
```

The above snippet references a Snowplow Analytics hosted version of the Snowplow JavaScript tracker v2.10.2 (//d1fc8wv8zag5ca.cloudfront.net/2.10.2/sp.js). Snowplow Analytics no longer hosts the latest versions of the Snowplow JavaScript tracker. It is recommended to self-host `sp.js` by following the [Self-hosting Snowplow.js](https://github.com/snowplow/snowplow/wiki/self-hosting-snowplow-js) guide.

For more details about setting up the tracker, please refer to the [official Snowplow Javascript Tracker Setup guide](https://github.com/snowplow/snowplow/wiki/Setting-up-a-Tracker).

To collect the data from the tracker, we need to setup Snowplow Collector. We'll use Scala Stream Collector. [Here the detailed guide](https://github.com/snowplow/snowplow/wiki/Setting-up-the-Scala-Stream-Collector) on how to install and configure it. [This repository with the Docker images](https://github.com/snowplow/snowplow-docker) for the Snowplow components is very helpful if you plan to deploy Snowplow with Docker.

Next, we need to install Snowplow Stream Enrich. Same as for collector, I
recommend following the [official guide](https://docs.snowplowanalytics.com/docs/pipeline-components-and-applications/enrichment-components/stream-enrich/install-stream-enrich/).

Finally, we need to have S3 Loader installed and configured to consume records
from AWS Kinesis and writes them to S3. You can follow [this guide](https://github.com/snowplow/snowplow/wiki/snowplow-s3-loader-setup) to set it up.

## Query S3 with Athena

Once we have data in S3 we can query it with AWS Athena or Presto. We’ll use Athena in our guide, but you can easily find a lot of materials online on how to set up an alternative configuration.

To query S3 data with Athena, we need to create a table for Snowplow events. Copy and paste the following DDL statement into the Athena console. Modify the LOCATION for the S3 bucket that stores your enriched Snowplow events.


```sql
CREATE EXTERNAL TABLE snowplow_events (
  app_id STRING,
  platform STRING,
  etl_tstamp TIMESTAMP,
  collector_tstamp TIMESTAMP,
  dvce_tstamp TIMESTAMP,
  event STRING,
  event_id STRING,
  txn_id INT,
  name_tracker STRING,
  v_tracker STRING,
  v_collector STRING,
  v_etl STRING,
  user_id STRING,
  user_ipaddress STRING,
  user_fingerprint STRING,
  domain_userid STRING,
  domain_sessionidx INT,
  network_userid STRING,
  geo_country STRING,
  geo_region STRING,
  geo_city STRING,
  geo_zipcode STRING,
  geo_latitude STRING,
  geo_longitude STRING,
  geo_region_name STRING,
  ip_isp STRING,
  ip_organization STRING,
  ip_domain STRING,
  ip_netspeed STRING,
  page_url STRING,
  page_title STRING,
  page_referrer STRING,
  page_urlscheme STRING,
  page_urlhost STRING,
  page_urlport INT,
  page_urlpath STRING,
  page_urlquery STRING,
  page_urlfragment STRING,
  refr_urlscheme STRING,
  refr_urlhost STRING,
  refr_urlport INT,
  refr_urlpath STRING,
  refr_urlquery STRING,
  refr_urlfragment STRING,
  refr_medium STRING,
  refr_source STRING,
  refr_term STRING,
  mkt_medium STRING,
  mkt_source STRING,
  mkt_term STRING,
  mkt_content STRING,
  mkt_campaign STRING,
  contexts STRING,
  se_category STRING,
  se_action STRING,
  se_label STRING,
  se_property STRING,
  se_value STRING,
  unstruct_event STRING,
  tr_orderid STRING,
  tr_affiliation STRING,
  tr_total STRING,
  tr_tax STRING,
  tr_shipping STRING,
  tr_city STRING,
  tr_state STRING,
  tr_country STRING,
  ti_orderid STRING,
  ti_sku STRING,
  ti_name STRING,
  ti_category STRING,
  ti_price STRING,
  ti_quantity INT,
  pp_xoffset_min INT,
  pp_xoffset_max INT,
  pp_yoffset_min INT,
  pp_yoffset_max INT,
  useragent STRING,
  br_name STRING,
  br_family STRING,
  br_version STRING,
  br_type STRING,
  br_renderengine STRING,
  br_lang STRING,
  br_features_pdf STRING,
  br_features_flash STRING,
  br_features_java STRING,
  br_features_director STRING,
  br_features_quicktime STRING,
  br_features_realplayer STRING,
  br_features_windowsmedia STRING,
  br_features_gears STRING,
  br_features_silverlight STRING,
  br_cookies STRING,
  br_colordepth STRING,
  br_viewwidth INT,
  br_viewheight INT,
  os_name STRING,
  os_family STRING,
  os_manufacturer STRING,
  os_timezone STRING,
  dvce_type STRING,
  dvce_ismobile STRING,
  dvce_screenwidth INT,
  dvce_screenheight INT,
  doc_charset STRING,
  doc_width INT,
  doc_height INT,
  tr_currency STRING,
  tr_total_base STRING,
  tr_tax_base STRING,
  tr_shipping_base STRING,
  ti_currency STRING,
  ti_price_base STRING,
  base_currency STRING,
  geo_timezone STRING,
  mkt_clickid STRING,
  mkt_network STRING,
  etl_tags STRING,
  dvce_sent_tstamp TIMESTAMP,
  refr_domain_userid STRING,
  refr_dvce_tstamp TIMESTAMP,
  derived_contexts STRING,
  domain_sessionid STRING,
  derived_tstamp TIMESTAMP
)
PARTITIONED BY(run STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE
LOCATION 's3://bucket-name/path/to/enriched/good';
```

Now, we're ready to connect Cube.js to Athena and start building our
application.
