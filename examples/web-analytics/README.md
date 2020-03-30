# Cube.js Web Analytics Template

Use this template to build your own open-source Google Analytics alternative. It’s developer friendly, hackable, and embeddable. It is easy to install the full working application and then customize every part of it from data collection to metrics definitions and visualizations. By self-hosting and managing the full lifecycle of the data you fully control the privacy and don’t need to send your users’ data to 3rd parties.

The example application uses Cube.js as the analytics backend, Snowplow for data collection, and Athena as the main data warehouse. The frontend is built with React, Material UI, and Recharts.

**Online demo:** [web-analytics-demo.cube.dev](https://web-analytics-demo.cube.dev/)

![](https://raw.githubusercontent.com/cube-js/cube.js/master/examples/web-analytics/screenshot.png)

**Ready-to-use.** Follow the installation guide below to install the whole stack from data collection to the frontend application. It comes with all Cube.js schema definitions for sessionization and attribution, as well as configured pre-aggregations for optimal performance.

**Hackable.** Use this template as a starting point to create your own web analytics platform. It’s designed to be completely customizable on every level. You can switch to a different data collection engine or build your own, use any SQL database as a warehouse, change how metrics are defined, and completely customize the frontend.

**Embeddable.** Backend components can be easily deployed as microservices into your existing stack. The frontend is a pure React application based on Material UI without any custom styles. You can embed any part of the frontend into your existing application and customize the look and feel to match your styles.

**Performance first.** The response time is under 50 ms by using Cube.js pre-aggregations. It scales well for tracking up to several million daily active users. To achieve this performance, Cube.js stores and manages aggregated tables in MySQL with a 5-minute refresh rate. You can learn more about performance optimization with external pre-aggregations in [this blog post](https://cube.dev/blog/when-mysql-is-faster-than-bigquery/).

## Installation

### 1. Configure Data Collection with Snowplow

The data collection part is handled by Snowplow. Follow Snowplow’s [Setup Guide](https://github.com/snowplow/snowplow/wiki/Setting-up-SnowPlow) to install the tracker, collector, and Enrich.

Snowplow comes with an S3 Loader and we recommend using it. Alternatively you can load your data in HDFS.

### 2. Set up Athena (S3 only) or Presto

Once you have data in the S3 or HDFS the next step is to set up Athena or Presto to query it. We’ll describe only Athena with an S3 setup here, but you can easily find a lot of materials online on how to set up an alternative configuration.

To query S3 data with Athena, we need to create a table for Snowplow events. Copy and paste the following DDL statement into the Athena console. Modify the `LOCATION` for the S3 bucket that stores your enriched Snowplow events.

<details>
  <summary>Show DDL statement</summary>
```sql
CREATE EXTERNAL TABLE atomic_events (
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
</details>

### 3. Install MySQL for Cube.js External Pre-Aggregations

This template uses MySQL as an external pre-aggregations database for performance optimization. Cube.js builds pre-aggregations from data stored in the main data warehouse, Athena in this example, and then uploads them into MySQL. Cube.js handles the refresh and partitioning of the pre-aggregations as well. 

You need to provide the following environment variables for Cube.js to connect to MySQL: `CUBEJS_EXT_DB_HOST`, `CUBEJS_EXT_DB_NAME`, `CUBEJS_EXT_DB_PORT`, `CUBEJS_EXT_DB_USER`, `CUBEJS_EXT_DB_PASS`. You can learn more about [external pre-aggregations in the documentation here.](https://cube.dev/docs/pre-aggregations#external-pre-aggregations)

### 4. Install Cube.js backend and React frontend applications

Docker container Configure via env variables

### 5. Enable Authentication via Google OAuth 2 (Optional)

You can enable authentication to let only users with emails with specific domains to sign in. To enable it, you need to define the following environment variables -

```bash
GOOGLE_AUTH_DOMAIN=DOMAIN-TO-ALLOW-SIGN-INS-FROM
GOOGLE_AUTH_REDIRECT=URL-OF-YOUR-DEPLOYMENT (e.g.: <https://analytics.myapp.com>)
GOOGLE_AUTH_CLIENT_ID=GOOGLE_CLIENT_ID
GOOGLE_AUTH_CLIENT_SECRET=GOOGLE_CLIENT_SECRET
```

To obtain `GOOGLE_CLIENT_ID` and `GOOGLE_CLIENT_SECRET` you must register an application with Google. If you have not already done so, a new project can be created in the [Google Developers Console](https://console.developers.google.com/). Your application will be issued a client ID and client secret. You will also need to configure a redirect URI, which should match the following pattern - `GOOGLE_AUTH_REDIRECT/auth/google/callback`, where `GOOGLE_AUTH_REDIRECT` is an environment variable you defined before.

### 6. Connect to your GraphQL API to Save Custom Reports (Optional)

Template uses Apollo GraphQL for CRUD operations related to custom reports. It uses `localStorage` currently for persistence. You can update GraphQL queries and mutations, as well as Apollo client configurations to use your GraphQL backend for persistent storage of custom reports.
