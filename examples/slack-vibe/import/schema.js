const schema = {
  users: {
    create:
      'CREATE TABLE users (' +
      'id TEXT PRIMARY KEY,' +
      'name TEXT,' +
      'title TEXT,' +
      'deleted INTEGER,' +
      'real_name TEXT,' +
      'image_512 TEXT,' +
      'is_admin INTEGER,' +
      'tz TEXT,' +
      'tz_offset TEXT' +
      ')',
    insert:
      'INSERT INTO users (' +
      'id, name, title, deleted, real_name, image_512, is_admin, tz, tz_offset' +
      ') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
  },
  channels: {
    create:
      'CREATE TABLE channels (' +
      'id TEXT PRIMARY KEY,' +
      'name TEXT,' +
      'is_archived INTEGER,' +
      'is_general INTEGER,' +
      'purpose TEXT' +
      ')',
    insert:
      'INSERT INTO channels (' +
      'id, name, is_archived, is_general, purpose' +
      ') VALUES (?, ?, ?, ?, ?)',
  },
  messages: {
    create:
      'CREATE TABLE messages (' +
      'id TEXT PRIMARY KEY,' +
      'channel_id TEXT,' +
      'type TEXT,' +
      'subtype TEXT,' +
      'text TEXT,' +
      'user_id TEXT,' +
      'ts REAL' +
      ')',
    insert:
      'INSERT INTO messages (' +
      'id, channel_id, type, subtype, text, user_id, ts' +
      ') VALUES (?, ?, ?, ?, ?, ?, ?)',
  },
  reactions: {
    create:
      'CREATE TABLE reactions (' +
      'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
      'message_id TEXT,' +
      'user_id TEXT,' +
      'emoji TEXT,' +
      'skin_tone TEXT' +
      ')',
    insert:
      'INSERT INTO reactions (' +
      'id, message_id, user_id, emoji, skin_tone' +
      ') VALUES (null, ?, ?, ?, ?)',
  },
};

module.exports = schema;
