require('dotenv').config();
const fs = require('fs');
const path = require('path');
const StreamZip = require('node-stream-zip');
const moment = require('moment-timezone');
const sqlite3 = require('sqlite3');
const { v4: uuid } = require('uuid');
const schema = require('./schema');

const dbPath = process.env.CUBEJS_DB_NAME;

function getRunQuery(db) {
  return async function runQuery(query, data = []) {
    return new Promise((resolve, reject) => {
      db.run(query, data, (err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  };
}

async function clearDatabase(runQuery) {
  // Drop tables
  for (const table of Object.keys(schema)) {
    await runQuery(`DROP TABLE IF EXISTS ${table}`);
  }

  // Create tables
  for (const table of Object.keys(schema)) {
    await runQuery(schema[table].create);
  }
}

function getReadZipEntry(zip) {
  const entries = Object.values(zip.entries());

  // Read the enclosing folder name
  const folder = entries[0].name.endsWith(path.sep)
    ? entries[0].name
    : path.dirname(entries[0].name) + '/';

  function readZipEntry(path) {
    return JSON.parse(zip.entryDataSync(folder + path).toString('utf8'));
  }

  function readZipFolder(name) {
    return entries
      .filter(
        (entry) => entry.name.indexOf(folder + name) === 0 && entry.isDirectory === false
      )
      .map((entry) => JSON.parse(zip.entryDataSync(entry.name).toString('utf8')))
      .reduce((all, one) => [...all, ...one], []);
  }

  return [readZipEntry, readZipFolder];
}

async function importUsers(runQuery, users) {
  for (const user of users) {
    await runQuery(schema.users.insert, [
      user.id,
      user.name,
      user.profile.title,
      user.deleted,
      user.real_name,
      user.profile.image_512,
      user.is_admin,
      user.tz,
      `UTC${moment.tz(user.tz).format('Z')}`,
    ]);
  }
}

async function importChannels(runQuery, channels) {
  for (const channel of channels) {
    await runQuery(schema.channels.insert, [
      channel.id,
      channel.name,
      channel.is_archived,
      channel.is_general,
      channel.purpose.value,
    ]);
  }
}

async function importMessages(runQuery, channelId, messages) {
  for (const message of messages) {
    const id = uuid();

    await runQuery(schema.messages.insert, [
      id,
      channelId,
      message.type,
      message.subtype,
      message.text,
      message.user,
      message.ts,
    ]);

    for (const reaction of message.reactions || []) {
      const parts = reaction.name.split('::');

      for (const user of reaction.users) {
        await runQuery(schema.reactions.insert, [
          id,
          user,
          parts[0],
          parts[1] || '',
        ]);
      }
    }
  }
}

async function tryInitDatabase() {
  console.log('Initializing database…');

  if (fs.existsSync(dbPath)) {
    console.log('Database file exists, import skipped.');
    return;
  }

  const runQuery = getRunQuery(new sqlite3.Database(dbPath));
  await clearDatabase(runQuery);
}

async function tryImportSlackArchive(path, onComplete) {
  console.log('Importing Slack archive…');

  if (!fs.existsSync(path)) {
    console.log(`No such file: ${path}`);
    return;
  }

  const runQuery = getRunQuery(new sqlite3.Database(dbPath));
  await clearDatabase(runQuery);

  const zip = new StreamZip({
    file: path,
    storeEntries: true,
  });

  zip.on('ready', async () => {
    const [readZipEntry, readZipFolder] = getReadZipEntry(zip);

    const users = readZipEntry('users.json');
    console.log(`Found ${users.length} users.`);
    await importUsers(runQuery, users);

    const channels = readZipEntry('channels.json');
    console.log(`Found ${channels.length} channels.`);
    await importChannels(runQuery, channels);

    for (const channel of channels) {
      const messages = readZipFolder(channel.name);
      console.log(
        `Importing data from #${
          channel.name
        }: ${
          messages.length
        } messages.`
      );
      await importMessages(runQuery, channel.id, messages);
    }

    zip.close();
    onComplete();
  });
}

module.exports = {
  tryInitDatabase,
  tryImportSlackArchive
};
