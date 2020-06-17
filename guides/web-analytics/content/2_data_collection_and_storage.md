---
order: 2
title: "Data Collection and Storage"
---

We're going to use Snowplow for data collection, S3 for storage and Athena to query the data in S3.

## Data Collection with Snowplow

Snowplow is an analytics platform to collect, enrich and store data.
We'll use Snowplow Javascript tracker on our website, which generates event-data and send it
to the Snowplow Collector to load to S3.

Before loading the data we'll use Enricher to turn IP addresses into coordinates. We'll use AWS Kinesis to manage data streams for collection, enrichment and then finally loading into S3. Schema below illustrates the whole process.

SCHEMA

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

The above snippet references a Snowplow Analytics hosted version of the Snowplow JavaScript tracker v2.10.2 (//d1fc8wv8zag5ca.cloudfront.net/2.10.2/sp.js). Snowplow Analytics no longer hosts the latest versions of the Snowplow JavaScript tracker. It is recommended to self-host `sp.js` by following Self hosting Snowplow js guide.

Here you can find the official Snowplow Javascript Tracker Setup guide.

To collect the data from the tracker we need to setup Snowplow Collector. We'll use Scala Stream Collector. Here the detailed guide on how to install and configure it. This repository with the Docker images for the Snowplow components is very helpful if you plan to deploy Snowplow with Docker.

Next, we need to install Snowplow Stream Enrich. Same as for collector, I
recommend following the official guide here and use these Docker images.

Finally, we need to have S3 Loader installed and configured to consume records
from AWS Kinesis and writes them to S3. You can follow [this guide](https://github.com/snowplow/snowplow/wiki/snowplow-s3-loader-setup) to set up it.

## Query S3 with Athena
