import cubejs from '@cubejs-client/core';
import emoji from 'node-emoji';
import moment from 'moment';
import url from 'url';

const cubejsApiUrl = process.env.NODE_ENV === 'production'
  ? `${window.location.origin}/cubejs-api/v1`
  : process.env.REACT_APP_CUBEJS_API;

function getServerUrl(uri) {
  const parts = url.parse(uri);
  return `${parts.protocol}//${parts.host}`;
}

const serverUrl = getServerUrl(cubejsApiUrl);

export const cubejsApi = cubejs(process.env.REACT_APP_CUBEJS_TOKEN, {
  apiUrl: cubejsApiUrl
});

export function checkHasData() {
  const query = {
    measures: ['Channels.count'],
  };

  return cubejsApi.load(query)
    .then(result => result.tablePivot().shift()['Channels.count'] > 0);
}

export function uploadSlackArchive(file) {
  const body = new FormData();
  body.append('file', file);

  return fetch(`${serverUrl}/upload`, {
    method: 'POST',
    body
  })
}

const membersQuery = {
  measures: ['Messages.count'],
  dimensions: [
    'Users.id',
    'Users.real_name',
    'Users.title',
    'Users.image',
    'Users.is_admin',
  ],
  order: { 'Messages.count': 'desc' },
};

export function loadMembers() {
  return cubejsApi.load(membersQuery).then((result) =>
    result
      .tablePivot()
      .map((row) => ({
        id: row['Users.id'],
        name: row['Users.real_name'],
        title: row['Users.title'],
        image: row['Users.image'],
        is_admin: row['Users.is_admin'],
      }))
      .filter((row) => row.id)
  );
}

const channelsQuery = {
  measures: ['Messages.count'],
  dimensions: ['Channels.id', 'Channels.name', 'Channels.purpose'],
  order: { 'Messages.count': 'desc' },
};

export function loadChannels() {
  return cubejsApi.load(channelsQuery).then((result) =>
    result
      .tablePivot()
      .map((row) => ({
        id: row['Channels.id'],
        name: row['Channels.name'],
        purpose: row['Channels.purpose'],
      }))
      .filter((row) => row.id)
  );
}

function mapReactions(row) {
  return Object.keys(row)
    .filter((key) => key.endsWith('.Reactions.count'))
    .sort((a, b) => (row[b] || 0) - (row[a] || 0))
    .map((key) => key.replace('.Reactions.count', ''))
    .filter((key) => emoji.findByName(key))
    .slice(0, 3)
    .map(emoji.get);
}

const reactionsByMembersQuery = {
  measures: ['Reactions.count'],
  dimensions: ['Reactions.emoji', 'Users.id'],
  order: { 'Reactions.count': 'desc' },
};

export function loadReactionsByMembers() {
  return cubejsApi.load(reactionsByMembersQuery).then((result) =>
    result
      .tablePivot({
        x: ['Users.id'],
        y: ['Reactions.emoji', 'measures'],
      })
      .map((row) => ({
        id: row['Users.id'],
        reactions: mapReactions(row),
      }))
  );
}

const reactionsInChannelsQuery = {
  measures: ['Reactions.count'],
  dimensions: ['Reactions.emoji', 'Channels.id'],
  order: { 'Reactions.count': 'desc' },
};

export function loadReactionsInChannels() {
  return cubejsApi.load(reactionsInChannelsQuery).then((result) =>
    result
      .tablePivot({
        x: ['Channels.id'],
        y: ['Reactions.emoji', 'measures'],
      })
      .map((row) => ({
        id: row['Channels.id'],
        reactions: mapReactions(row),
      }))
  );
}

function loadStuffWithReactions(loadStuff, loadReactions) {
  return Promise.all([loadStuff(), loadReactions()]).then(
    ([stuff, reactions]) =>
      stuff.map((item) => {
        const row = reactions.find((row) => row['id'] === item.id);

        return {
          ...item,
          reactions: row ? row.reactions : [],
        };
      })
  );
}

export function loadMembersWithReactions() {
  return loadStuffWithReactions(loadMembers, loadReactionsByMembers);
}

export function loadChannelsWithReactions() {
  return loadStuffWithReactions(loadChannels, loadReactionsInChannels);
}

function createFilters(channel, member) {
  return [
    ...(channel
      ? [
          {
            member: 'Channels.name',
            operator: 'equals',
            values: [channel],
          },
        ]
      : []),
    ...(member
      ? [
          {
            member: 'Users.real_name',
            operator: 'equals',
            values: [member],
          },
        ]
      : []),
  ];
}

export function loadMessagesAndReactions(
  dateRange,
  granularity,
  channel,
  member
) {
  const query = {
    measures: ['Messages.count', 'Reactions.count'],
    timeDimensions: [
      {
        dimension: 'Messages.date',
        granularity,
        dateRange,
      },
    ],
    filters: createFilters(channel, member),
    order: { 'Messages.date': 'asc' },
  };

  return cubejsApi.load(query).then((result) =>
    result.tablePivot().map((row) => ({
      date: new Date(row['Messages.date.' + granularity]),
      month: moment(row['Messages.date.' + granularity]).format('MMM'),
      weekday: moment(row['Messages.date.' + granularity]).format('dddd'),
      messages: parseInt(row['Messages.count']),
      reactions: parseInt(row['Reactions.count']),
    }))
  );
}

export function loadMembersAndJoins(dateRange, granularity, channel, member) {
  const query = {
    measures: ['Memberships.sum', 'Memberships.count'],
    timeDimensions: [
      {
        dimension: 'Messages.date',
        granularity,
        dateRange,
      },
    ],
    filters: createFilters(channel, member),
    order: { 'Messages.date': 'asc' },
  };

  return cubejsApi.load(query).then((result) =>
    result.tablePivot().map((row) => ({
      date: new Date(row['Messages.date.' + granularity]),
      members: parseInt(row['Memberships.sum']),
      joins: parseInt(row['Memberships.count']),
    }))
  );
}

export function loadMessagesByWeekday(dateRange, channel, member) {
  const query = {
    measures: ['Messages.count'],
    dimensions: ['Messages.day_of_week'],
    timeDimensions: [
      {
        dimension: 'Messages.date',
        granularity: 'month',
        dateRange,
      },
    ],
    filters: createFilters(channel, member),
    order: { 'Messages.day_of_week': 'asc' },
  };

  const granularity = query.timeDimensions[0].granularity;
  return cubejsApi.load(query).then((result) => {
    return result.tablePivot().map((row) => ({
      month: moment(row['Messages.date.' + granularity]).format('MMM'),
      weekday: moment().weekday(row['Messages.day_of_week']).format('dddd'),
      value: parseInt(row['Messages.count']),
    }));
  });
}

export function loadMessagesByHour(dateRange, channel, member) {
  const query = {
    measures: ['Messages.count'],
    dimensions: ['Messages.hour', 'Messages.day_of_week'],
    timeDimensions: [
      {
        dimension: 'Messages.date',
        dateRange,
      },
    ],
    filters: createFilters(channel, member),
    order: { 'Messages.day_of_week': 'asc', 'Messages.hour': 'asc' },
  };

  return cubejsApi.load(query).then((result) => {
    return result.tablePivot().map((row) => ({
      hour: row['Messages.hour'] + ':00',
      weekday: moment().weekday(row['Messages.day_of_week']).format('dddd'),
      value: parseInt(row['Messages.count']),
    }));
  });
}

const messagesByChannelQuery = {
  measures: ['Messages.count'],
  dimensions: ['Channels.name'],
  order: { 'Channels.name': 'desc' },
};

export function loadMessagesByChannel() {
  return cubejsApi.load(messagesByChannelQuery).then((result) => {
    return result.tablePivot().map((row) => ({
      title: row['Channels.name'],
      value: parseInt(row['Messages.count']),
    }));
  });
}

const membersByChannelQuery = {
  measures: ['Memberships.count'],
  dimensions: ['Channels.name'],
  order: { 'Channels.name': 'desc' },
};

export function loadMembersByChannel() {
  return cubejsApi.load(membersByChannelQuery).then((result) => {
    return result.tablePivot().map((row) => ({
      title: row['Channels.name'],
      value: parseInt(row['Memberships.count']),
    }));
  });
}

const membersByTimezoneQuery = {
  measures: ['Users.count'],
  dimensions: ['Users.tz_offset'],
};

export function loadMembersByTimezone() {
  return cubejsApi.load(membersByTimezoneQuery).then((result) =>
    result.tablePivot().map((row) => {
      return {
        id: row['Users.tz_offset'],
        value: row['Users.count'],
      };
    })
  );
}
