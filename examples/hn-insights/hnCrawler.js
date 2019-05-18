const scrapeIt = require('scrape-it');
const R = require('ramda');
const zlib = require('zlib');
const AWS = require('aws-sdk');
const redis = require('redis');
const { promisify } = require('util');
const humps = require('humps');
const AthenaDriver = require('@cubejs-backend/athena-driver');
const promiseRetry = require('promise-retry');
const fetch = require('node-fetch');

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

const scrapeUser = async (userId) => {
  const { data, response, body } = await scrapeIt(`https://news.ycombinator.com/user?id=${userId}`, {
    properties: {
      listItem: 'table table tr',
      data: {
        name: {
          selector: 'td:first-child'
        },
        value: {
          selector: 'td:last-child'
        }
      }
    }
  });
  if (response.statusCode === 403) {
    throw new Error(`Banned`);
  }
  if (response.statusCode !== 200) {
    console.log(response.statusCode);
    console.log(body);
    throw new Error(`Error fetching user: ${userId}, status: ${response.statusCode}`);
  }
  const result = data.properties
    .map(({ name, value }) => (name.match(/(.*):/) && { [name.match(/(.*):/)[1]]: value }))
    .filter(p => !!p)
    .reduce((a, b) => ({ ...a, ...b }));
  if (result.user !== userId) {
    console.log(response.statusCode);
    console.log(body);
    throw new Error(`User ${userId} fetch problem`);
  }
  return result;
};

const fetchUser = async (userId) => {
  return (await fetch(`https://hacker-news.firebaseio.com/v0/user/${userId}.json`)).json();
};

const fetchStory = async (storyId) => {
  return (await fetch(`https://hacker-news.firebaseio.com/v0/item/${storyId}.json`)).json();
};

exports.scrapeUser = scrapeUser;

const cached = async (key, getFn, expire) => {
  let value = await redisClient.getAsync(key);
  value = value !== 'undefined' && JSON.parse(value) || null;
  if (!value) {
    value = await getFn();
    await redisClient.setAsync(key, JSON.stringify(value), 'EX', expire || 3600);
  }
  return value;
};

const sleep = (timeout) => new Promise(resolve => setTimeout(() => resolve(), timeout));

const scrapePages = async (baseUrl, url, pages) => {
  const result = [];
  let moreLink = null;
  for (let i = 0; i < pages; i++) {
    console.log(`Scraping ${url}: page ${i}`);
    const { data } = await scrapeStoryList(moreLink && `${baseUrl}${moreLink}` || url);
    const stories = toStoriesData(data);
    await stories.map((s) => async () => {
      const user = s.user && await cached(
        `HN_INSIGHTS_USER_${s.user}_${s.score}`,
        async () => {
          const res = await promiseRetry(async (retry, number) => {
            if (number > 1) {
              console.log(`Retrying ${s.user} fetch...${number}`);
            }
            try {
              return await fetchUser(s.user);
            } catch (e) {
              if (e.message === 'Banned') {
                console.log(`Skipping user ${s.user} due to DDoS ban...`);
                return {}; // TODO skip user for 10 minutes
              }
              await retry(e);
            }
          }, { retries: 4 });
          // await sleep(300);
          return res;
        },
        10 * 60
      );
      result.push({
        ...s,
        karma: user && parseInt(user.karma || '0', 10)
      });
    }).reduce((a, b) => a.then(b), Promise.resolve());
    // eslint-disable-next-line prefer-destructuring
    moreLink = data.moreLink;
  }
  return result;
};

exports.scrapeFrontPage =
  async () => scrapePages('https://news.ycombinator.com/', 'https://news.ycombinator.com/', 5);

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
      ['rank', 'score', 'commentsCount', 'karma'].map(p => intDiff(p, idToOldStories[id], idToNewStories[id])))
      .reduce((a, b) => ({ ...a, ...b })),
    prevSnapshotTimestamp: idToOldStories[id].snapshotTimestamp
  }));
  const timestamp = new Date().toISOString();
  return addedStories.map(s => ({ ...s, event: 'added', timestamp })).concat(
    removedStories.map(s => ({ ...s, event: 'removed', timestamp }))
  ).concat(changedStories.map(s => ({ ...s, event: 'changed', timestamp })));
};

const hnInsightsStateKey = 'HN_INSIGHTS_STATE';

const storyAddedEventRedisKey = (s) => `${hnInsightsStateKey}_ADDED_TO_FRONT_EVENT_${s.id}`;

const changeEvents = async (state, page, listFn) => {
  const oldList = state[page];
  let newList = await listFn();
  if (page === 'front') {
    newList = await Promise.all(newList.map(async (s) => {
      const storyFromApi = await cached(`${hnInsightsStateKey}_STORY_${s.id}`, () => fetchStory(s.id), 60 * 60);
      const addedToFrontEvent = JSON.parse(await redisClient.getAsync(storyAddedEventRedisKey(s)));
      const oldStory = oldList && oldList.find(old => old.id === s.id);
      if (storyFromApi && s.score) {
        let timeBase = new Date(s.snapshotTimestamp).getTime() - new Date(storyFromApi.time * 1000).getTime();
        // second chance pool
        if (timeBase / (1000 * 60 * 60) > 10 && s.rank < 60 && addedToFrontEvent) {
          timeBase = new Date(s.snapshotTimestamp).getTime() - new Date(addedToFrontEvent.snapshotTimestamp).getTime();
        }
        const originalRankScore = Math.pow(s.score - 1, 0.8) / Math.pow(
          timeBase / (1000 * 60 * 60) + 2,
          1.8
        );
        const penalty = oldStory && oldStory.penalty || 1;
        const rankScore = originalRankScore * penalty;
        return {
          ...s,
          originalRankScore,
          rankScore,
          penalty,
          createdTimestamp: new Date(storyFromApi.time * 1000).toISOString(),
          addedToFrontTimestamp: addedToFrontEvent && addedToFrontEvent.snapshotTimestamp
        };
      } else {
        return s;
      }
    }));
    newList = newList.map(s => {
      let prevStory = newList.find(prev => prev.rank === s.rank - 1);
      if (prevStory && !prevStory.rankScore) {
        prevStory = newList.find(prev => prev.rank === s.rank - 2); // TODO
      }
      if (s.originalRankScore) {
        const penalty =
          s.rank === 1 ? 1 : (
            prevStory &&
            prevStory.rankScore && (
              prevStory.rankScore / s.originalRankScore < 0.1 ?
                Math.min(Math.round(100.0 * prevStory.rankScore / s.originalRankScore) / 100, 1) :
                Math.min(Math.round(10.0 * prevStory.rankScore / s.originalRankScore) / 10, 1)
            )
          );
        // penalty = Math.min(s.penalty || 1, penalty); TODO
        return penalty ? {
          ...s,
          penalty,
          rankScore: s.originalRankScore * penalty
        } : s;
      } else {
        return s;
      }
    });
  }
  const diff = await exports.storyListEventDiff(oldList || [], newList);
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

exports.generateChangeEvents = async () => {
  const state = JSON.parse(await redisClient.getAsync(hnInsightsStateKey)) || {};
  const newestDiff = await changeEvents(
    state,
    'newest',
    async () => (await exports.scrapeNewest()).map(({ rank, ...s }) => ({ ...s }))
  );

  const frontPageDiff = await changeEvents(state, 'front', exports.scrapeFrontPage);

  const addedToFront = frontPageDiff.filter(e => e.event === 'added');
  await Promise.all(
    addedToFront.map(
      s => redisClient.setAsync(storyAddedEventRedisKey(s), JSON.stringify(s), 'EX', 60 * 60 * 24 * 10)
    )
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
