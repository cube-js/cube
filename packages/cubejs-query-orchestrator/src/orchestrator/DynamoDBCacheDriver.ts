const DynamoDB = require('aws-sdk/clients/dynamodb');

const DocumentClient = new DynamoDB.DocumentClient();

const { Table, Entity } = require('dynamodb-toolbox');

const TTL_KEY = process.env.DYNAMODB_TTL_KEY ?? 'exp';

export class DynamoDBCacheDriver {
  public readonly tableName: string;

  public readonly table: any;

  public readonly cache: any;

  constructor({ tableName }) {
    this.tableName = tableName ?? process.env.CUBEJS_CACHE_TABLE;

    this.table = new Table({
      // Specify table name (used by DynamoDB)
      name: this.tableName,

      // Define partition key
      partitionKey: 'pk',
      sortKey: 'sk',

      // Add the DocumentClient
      DocumentClient
    });

    this.cache = new Entity({
      // Specify entity name
      name: 'Cache',

      // Define attributes
      attributes: {
        key: { partitionKey: true }, // flag as partitionKey
        sk: { hidden: true, sortKey: true, type: 'string' }, // flag as sortKey and mark hidden because we do not care about it?
        value: { type: 'string' }, // set the attribute type to string
        [`${TTL_KEY}`]: { type: 'number' } // set the attribute type to number for ttl
      },

      // Assign it to our table
      table: this.table
    });
  }

  public async get(key: string) {
    const result = await this.cache.get({ key, sk: key });
    return result && result.Item && JSON.parse(result.Item.value);
  }

  public async set(key: string, value: any, expiration: number) {
    const item = {
      key,
      sk: key,
      value: JSON.stringify(value),
      [`${TTL_KEY}`]: (new Date().getTime() + expiration) / 1000 // needs to be in seconds
    };

    await this.cache.put(item);
  }

  public async remove(key: string) {
    await this.cache.delete({ key, sk: key });
  }

  public async keysStartingWith(prefix) {
    // TODO: fix this
    console.log('### KEYS STARTING WITH RESULT');

    const result = await this.table.scan({
      limit: 100, // limit to 100 items
      filters: [
        { attr: 'key', beginsWith: prefix },
        { attr: TTL_KEY, lt: new Date().getTime() } // only return items with TTL less than now
      ], 
    });
    
    return result;
  }
}
