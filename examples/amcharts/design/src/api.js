import cubejs from '@cubejs-client/core';
import emoji from 'node-emoji';
import moment from 'moment';

export const cubejsApi = cubejs(process.env.REACT_APP_CUBEJS_TOKEN, {
  apiUrl: process.env.REACT_APP_CUBEJS_API,
});

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
    result.tablePivot()
      .map((row) => ({
        id: row['Users.id'],
        name: row['Users.real_name'],
        title: row['Users.title'],
        image: row['Users.image'],
        is_admin: row['Users.is_admin'],
      }))
      .filter(row => row.id)
  );
}

const channelsQuery = {
  measures: ['Messages.count'],
  dimensions: ['Channels.id', 'Channels.name', 'Channels.purpose'],
  order: { 'Messages.count': 'desc' },
};

export function loadChannels() {
  return cubejsApi.load(channelsQuery).then((result) =>
    result.tablePivot()
      .map((row) => ({
        id: row['Channels.id'],
        name: row['Channels.name'],
        purpose: row['Channels.purpose'],
      }))
      .filter(row => row.id)
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

const messagesAndReactionsQuery = {
  measures: ['Messages.count', 'Reactions.count'],
  timeDimensions: [
    {
      dimension: 'Messages.date',
      granularity: 'month',
      dateRange: 'Last 365 days',
    },
  ],
  order: { 'Messages.date': 'asc' },
};

export function loadMessagesAndReactions() {
  const granularity = messagesAndReactionsQuery.timeDimensions[0].granularity;

  return cubejsApi.load(messagesAndReactionsQuery).then((result) =>
    result.tablePivot().map((row) => ({
      date: new Date(row['Messages.date.' + granularity]),
      month: moment(row['Messages.date.' + granularity]).format('MMM'),
      weekday: moment(row['Messages.date.' + granularity]).format('dddd'),
      messages: parseInt(row['Messages.count']),
      reactions: parseInt(row['Reactions.count']),
    }))
  );
}

const membersAndJoinsQuery = {
  measures: ['Memberships.sum', 'Memberships.count'],
  timeDimensions: [
    {
      dimension: 'Messages.date',
      granularity: 'month',
      dateRange: 'Last 365 days',
    },
  ],
  order: { 'Messages.date': 'asc' },
};

export function loadMembersAndJoins() {
  const granularity = membersAndJoinsQuery.timeDimensions[0].granularity;

  return cubejsApi.load(membersAndJoinsQuery).then((result) =>
    result.tablePivot().map((row) => ({
      date: new Date(row['Messages.date.' + granularity]),
      members: parseInt(row['Memberships.sum']),
      joins: parseInt(row['Memberships.count']),
    }))
  );
}

const messagesByWeekdayQuery = {
  measures: ['Messages.count'],
  dimensions: ['Messages.day_of_week'],
  timeDimensions: [
    {
      dimension: 'Messages.date',
      granularity: 'month',
      dateRange: 'Last 365 days',
    },
  ],
  order: { 'Messages.day_of_week': 'asc' },
};

export function loadMessagesByWeekday() {
  const granularity = messagesByWeekdayQuery.timeDimensions[0].granularity;
  return cubejsApi.load(messagesByWeekdayQuery).then((result) => {
    return result.tablePivot().map((row) => ({
      month: moment(row['Messages.date.' + granularity]).format('MMM'),
      weekday: moment().weekday(row['Messages.day_of_week']).format('dddd'),
      value: parseInt(row['Messages.count']),
    }));
  });
}

const messagesByHourQuery = {
  measures: ['Messages.count'],
  dimensions: ['Messages.hour', 'Messages.day_of_week'],
  order: { 'Messages.day_of_week': 'asc', 'Messages.hour': 'asc' },
};

export function loadMessagesByHour() {
  return cubejsApi.load(messagesByHourQuery).then((result) => {
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
