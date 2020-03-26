Modular and hackable open source web analytics platform.

## How is it Different from Google Analytics or Matomo?

Unlike Google Analytics it is free and open-source.

Unlike Matomo, it not a monolithic application, but rather a set of modules,
which based on different established open-source technologies: Snowplow,
PrestoDB, Cube.js, and React with Material UI on frontend. You can change or
replace any component with technology that better fits your needs. Additionally, due to Cube.js data schema you can customize how metrics and dimensions are defined in a case the default definitions don't work for you.

## Architecture

TODO

## Online Demo

Check out the online demo at [web-analytics-demo.cube.dev](https://web-analytics-demo.cube.dev)

## Installation

### 1. Configure Data Collection with Snowplow

The data collection part is handled by Snowplow. Follow the Snowplow's [Setup Guide](https://github.com/snowplow/snowplow/wiki/Setting-up-SnowPlow) to install tracker, collector and Enrich.

Snowplow comes with an S3 Loader and we recommend using it. Alternatively you
can load your data in HDFS.

### 2. Setup Athena (S3 only) or Presto

Once you have data in the S3 or HDFS the next step is to setup Athena or Presto
to query it. We'll describe only Athena with S3 setup here, but you can easily find a
lot of materials online how to setup an alternative configuration.

To query S3 data with Athena we need to create a table for Snowplow events. Copy and paste the following DDL statement into the Athena console. Modify the `LOCATION` for the S3 bucket that stores your enriched Snowplow events.

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
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://bucket-name/path/to/enriched/good';
```
</details>

### 3. Install MySQL for Cube.js External Pre-Aggregations

### 4. Install Cube.js backend and React frontend applications
Docker container
Configure via env variables

### Enable Authentication via Google OAuth 2 (Optional)

You can enable authentication to let only users with emails with specific domain
to sign in. To enable it you need to define the following environment variables -

```bash
GOOGLE_AUTH_DOMAIN=DOMAIN-TO-ALLOW-SIGN-INS-FROM
GOOGLE_AUTH_REDIRECT=URL-OF-YOUR-DEPLOYMENT (e.g.: https://analytics.myapp.com)
GOOGLE_AUTH_CLIENT_ID=GOOGLE_CLIENT_ID
GOOGLE_AUTH_CLIENT_SECRET=GOOGLE_CLIENT_SECRET
```

To obtain `GOOGLE_CLIENT_ID` and `GOOGLE_CLIENT_SECRET` you must register an application with Google. If you have not already done so, a new project can be created in the [Google Developers Console](https://console.developers.google.com/). Your application will be issued a client ID and client secret. You will also need to configure a redirect URI which should match the following patter - `GOOGLE_AUTH_REDIRECT/auth/google/callback`, where `GOOGLE_AUTH_REDIRECT` is an environment variable you defined before.

## Future Development

* Support multiple tracking applications with [Cube.js multitenancy](https://cube.dev/docs/multitenancy-setup).
* Support geo dimensions and map chart.
* Add filters to custom report builder.

## Contributing

## License
