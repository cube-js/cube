const scrapeIt = require('scrape-it');
const R = require('ramda');
const zlib = require('zlib');
const AWS = require('aws-sdk');
const redis = require('redis');
const { promisify } = require('util');
const humps = require('humps');
const AthenaDriver = require('@cubejs-backend/athena-driver');

const redisClient = redis.createClient(process.env.REDIS_URL);

['get', 'set'].forEach(
  k => {
    redisClient[`${k}Async`] = promisify(redisClient[k]);
  }
);

const s3 = new AWS.S3();

const toStoriesData = (data) => {
  const { stories } = data;
  const snapshotTimestamp = new Date().toISOString();
  return stories.map(({ commentAndHoursAgo, ...story }) => ({
    ...story,
    rank: story.rank && parseInt(story.rank.match(/(\d+)\./)[1] || '0', 10),
    score: story.score && parseInt(story.score.match(/(\d+) point/)[1] || '0', 10),
    commentsCount: commentAndHoursAgo && parseInt(
      commentAndHoursAgo.match(/(\d+)\s+comments/) &&
      commentAndHoursAgo.match(/(\d+)\s+comments/)[1] || '0',
      10
    ),
    snapshotTimestamp
  }));
};

const scrapeStoryList = async (url) => scrapeIt(url, {
  stories: {
    listItem: ".athing",
    data: {
      id: {
        attr: 'id'
      },
      href: {
        attr: 'href',
        selector: 'a.storylink'
      },
      rank: '.rank',
      title: 'a.storylink',
      score: {
        selector: '.title',
        how: (elm) => elm.parent().next().find('.score').text()
      },
      user: {
        selector: '.title',
        how: (elm) => elm.parent().next().find('.hnuser').text()
      },
      commentAndHoursAgo: {
        selector: '.title',
        how: (elm) => elm.parent().next().find('a[href*=item]').text()
      }
    }
  },
  moreLink: {
    selector: 'a.morelink',
    attr: 'href'
  }
});

const scrapePages = async (baseUrl, url, pages) => {
  let result = [];
  let moreLink = null;
  for (let i = 0; i < pages; i++) {
    const { data } = await scrapeStoryList(moreLink && `${baseUrl}${moreLink}` || url);
    result = result.concat(toStoriesData(data));
    // eslint-disable-next-line prefer-destructuring
    moreLink = data.moreLink;
  }
  return result;
};

exports.scrapeFrontPage =
  async () => scrapePages('https://news.ycombinator.com/', 'https://news.ycombinator.com/', 10);

exports.scrapeNewest =
  async () => scrapePages('https://news.ycombinator.com/', 'https://news.ycombinator.com/newest', 5);

exports.storyListEventDiff = async (oldStories, newStories) => {
  const cmpById = (x, y) => x.id === y.id;
  const idToObject = R.pipe(
    R.map(s => [s.id, s]),
    R.fromPairs
  );
  const intDiff = (prop, o, n) => ({ [`${prop}Diff`]: n[prop] != null && o[prop] != null && (n[prop] - o[prop]) || 0 });
  const addedStories = R.differenceWith(cmpById, newStories, oldStories);
  const removedStories = R.differenceWith(cmpById, oldStories, newStories);
  const idToOldStories = idToObject(oldStories);
  const idToNewStories = idToObject(newStories);
  const idIntersection = R.intersection(
    oldStories.map(s => s.id),
    newStories.map(s => s.id)
  );
  const changedStories = idIntersection.filter(
    id => !R.equals(idToNewStories[id], idToOldStories[id])
  ).map(id => ({
    ...idToNewStories[id],
    ...(
      ['rank', 'score', 'commentsCount'].map(p => intDiff(p, idToOldStories[id], idToNewStories[id])))
      .reduce((a, b) => ({ ...a, ...b })),
    prevSnapshotTimestamp: idToOldStories[id].snapshotTimestamp
  }));
  const timestamp = new Date().toISOString();
  return addedStories.map(s => ({ ...s, event: 'added', timestamp })).concat(
    removedStories.map(s => ({ ...s, event: 'removed', timestamp }))
  ).concat(changedStories.map(s => ({ ...s, event: 'changed', timestamp })));
};

const changeEvents = async (state, page, listFn) => {
  const newList = await listFn();
  const diff = await exports.storyListEventDiff(state[page] || [], newList);
  state[page] = newList;
  return diff.map(e => ({ ...e, page }));
};

const uploadEvents = async (events) => {
  const outStream = zlib.createGzip();
  events.forEach(e => {
    outStream.write(`${JSON.stringify(humps.decamelizeKeys(e))}\n`, 'utf8');
  });
  outStream.end();
  const date = new Date().toISOString();
  const partitionPrefix = date.substring(0, 13);
  const fileName = `dt=${partitionPrefix}/${date}.json.gz`;
  const params = {
    Bucket: process.env.HN_INSIGHTS_EVENTS_BUCKET || 'hn-insights-events',
    Key: fileName,
    Body: outStream
  };
  console.log(`Uploading ${fileName}: ${events.length} events...`);
  await s3.upload(params).promise();
  console.log(`Uploading ${fileName} done`);
};

const hnInsightsStateKey = 'HN_INSIGHTS_STATE';

exports.generateChangeEvents = async () => {
  const state = JSON.parse(await redisClient.getAsync(hnInsightsStateKey)) || {};
  const frontPageDiff = await changeEvents(state, 'front', exports.scrapeFrontPage);
  const newestDiff = await changeEvents(
    state,
    'newest',
    async () => (await exports.scrapeNewest()).map(({ rank, ...s }) => ({ ...s }))
  );
  const result = frontPageDiff.concat(newestDiff);
  if (result.length) {
    await uploadEvents(result);
  }
  await redisClient.setAsync(hnInsightsStateKey, JSON.stringify(state));
  return result;
};

exports.refreshPartitions = async () => {
  const athenaDriver = new AthenaDriver();
  await athenaDriver.query('MSCK REPAIR TABLE hn_insights.events');
  return {
    statusCode: 200
  };
};

exports.schedule = async () => {
  await exports.generateChangeEvents();
  return {
    statusCode: 200
  };
};

exports.debugTimer = () => {
  setInterval(
    async () => console.log(await exports.generateChangeEvents()),
    15000
  );
};
