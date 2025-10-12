/* globals describe, test, expect, beforeEach */
const sqlite3 = require('sqlite3');
const SqliteDriver = require('../driver/SqliteDriver.js');

describe('SqliteDriver', () => {
  let driver;

  beforeEach(() => {
    const db = new sqlite3.Database(':memory:');
    driver = new SqliteDriver({ db });
  });

  test('testConnection', async () => {
    await driver.testConnection();
  });

  test('tableSchema', async () => {
    await driver.query(`
       CREATE TABLE users (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         name TEXT NOT NULL,
         email TEXT UNIQUE NOT NULL,
         age INTEGER,
         created_at DATETIME DEFAULT CURRENT_TIMESTAMP
       );
    `);

    await driver.query(`
       CREATE TABLE groups (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         name TEXT NOT NULL
       );
    `);

    const tableSchema = await driver.tablesSchema();

    expect(tableSchema).toEqual({
      main: {
        users: [
          { name: 'id', type: 'INTEGER' },
          { name: 'name', type: 'TEXT' },
          { name: 'email', type: 'TEXT' },
          { name: 'age', type: 'INTEGER' },
          { name: 'created_at', type: 'DATETIME' },
        ],
        groups: [
          { name: 'id', type: 'INTEGER' },
          { name: 'name', type: 'TEXT' },
        ]
      }
    });
  });
});
