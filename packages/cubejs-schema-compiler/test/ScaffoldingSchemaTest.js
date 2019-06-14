const ScaffoldingSchema = require('../scaffolding/ScaffoldingSchema');
const ScaffoldingTemplate = require('../scaffolding/ScaffoldingTemplate');
require('should');

const driver = {
  quoteIdentifier: (name) => `"${name}"`
};

describe('ScaffoldingSchema', () => {
  it('schema', () => {
    const schema = new ScaffoldingSchema({
      public: {
        orders: [{
          "name": "id",
          "type": "integer",
          "attributes": []
        }, {
          "name": "amount",
          "type": "integer",
          "attributes": []
        }, {
          "name": "customer_id",
          "type": "integer",
          "attributes": []
        }],
        customers: [{
          "name": "id",
          "type": "integer",
          "attributes": []
        }, {
          "name": "name",
          "type": "character varying",
          "attributes": []
        }, {
          "name": "account_id",
          "type": "integer",
          "attributes": []
        }],
        accounts: [{
          "name": "id",
          "type": "integer",
          "attributes": []
        }, {
          "name": "username",
          "type": "character varying",
          "attributes": []
        }, {
          "name": "password",
          "type": "character varying",
          "attributes": []
        }, {
          "name": "failure_count",
          "type": "integer",
          "attributes": []
        }, {
          "name": "account_status",
          "type": "character varying",
          "attributes": []
        }],
      }
    }, driver);
    const schemaForTables = schema.generateForTables(['public.orders', 'public.customers', 'public.accounts']);
    schemaForTables.should.be.deepEqual([
      {
        "cube": "Orders",
        "schema": "public",
        "table": "orders",
        "tableName": "public.orders",
        "measures": [
          {
            "name": "amount",
            "types": [
              "sum",
              "avg",
              "min",
              "max"
            ],
            "title": "Amount"
          }
        ],
        "dimensions": [
          {
            "name": "id",
            "types": [
              "number"
            ],
            "title": "Id",
            "isPrimaryKey": true
          }
        ],
        "drillMembers": [
          {
            "name": "id",
            "types": [
              "number"
            ],
            "title": "Id",
            "isPrimaryKey": true
          }
        ],
        "joins": [
          {
            "thisTableColumn": "customer_id",
            "tableName": "public.customers",
            "cubeToJoin": "Customers",
            "columnToJoin": "id",
            "relationship": "belongsTo"
          }
        ]
      },
      {
        "cube": "Customers",
        "schema": "public",
        "table": "customers",
        "tableName": "public.customers",
        "measures": [],
        "dimensions": [
          {
            "name": "id",
            "types": [
              "number"
            ],
            "title": "Id",
            "isPrimaryKey": true
          },
          {
            "name": "name",
            "types": [
              "string"
            ],
            "title": "Name",
            "isPrimaryKey": false
          }
        ],
        "drillMembers": [
          {
            "name": "id",
            "types": [
              "number"
            ],
            "title": "Id",
            "isPrimaryKey": true
          },
          {
            "name": "name",
            "types": [
              "string"
            ],
            "title": "Name",
            "isPrimaryKey": false
          }
        ],
        "joins": [
          {
            "thisTableColumn": "account_id",
            "tableName": "public.accounts",
            "cubeToJoin": "Accounts",
            "columnToJoin": "id",
            "relationship": "belongsTo"
          }
        ]
      },
      {
        "cube": "Accounts",
        "schema": "public",
        "table": "accounts",
        "tableName": "public.accounts",
        "measures": [
          {
            "name": "failure_count",
            "types": [
              "sum",
              "avg",
              "min",
              "max"
            ],
            "title": "Failure Count"
          }
        ],
        "dimensions": [
          {
            "name": "id",
            "types": [
              "number"
            ],
            "title": "Id",
            "isPrimaryKey": true
          },
          {
            "name": "username",
            "types": [
              "string"
            ],
            "title": "Username",
            "isPrimaryKey": false
          },
          {
            "name": "password",
            "types": [
              "string"
            ],
            "title": "Password",
            "isPrimaryKey": false
          },
          {
            "name": "account_status",
            "types": [
              "string"
            ],
            "title": "Account Status",
            "isPrimaryKey": false
          }
        ],
        "drillMembers": [
          {
            "name": "id",
            "types": [
              "number"
            ],
            "title": "Id",
            "isPrimaryKey": true
          },
          {
            "name": "username",
            "types": [
              "string"
            ],
            "title": "Username",
            "isPrimaryKey": false
          }
        ],
        "joins": []
      }
    ]);
  });

  it('template', () => {
    const template = new ScaffoldingTemplate({
      public: {
        orders: [{
          "name": "id",
          "type": "integer",
          "attributes": []
        }, {
          "name": "amount",
          "type": "integer",
          "attributes": []
        }, {
          "name": "customerId",
          "type": "integer",
          "attributes": []
        }],
        customers: [{
          "name": "id",
          "type": "integer",
          "attributes": []
        }, {
          "name": "visit_count",
          "type": "integer",
          "attributes": []
        }, {
          "name": "name",
          "type": "character varying",
          "attributes": []
        }, {
          "name": "accountId",
          "type": "integer",
          "attributes": []
        }],
        accounts: [{
          "name": "id",
          "type": "integer",
          "attributes": []
        }, {
          "name": "username",
          "type": "character varying",
          "attributes": []
        }, {
          "name": "password",
          "type": "character varying",
          "attributes": []
        }, {
          "name": "failureCount",
          "type": "integer",
          "attributes": []
        }]
      }
    }, driver);
    template.generateFilesByTableNames(['public.orders', 'public.customers', 'public.accounts']).should.be.deepEqual([
      {
        fileName: 'Orders.js',
        content: `cube(\`Orders\`, {
  sql: \`SELECT * FROM public.orders\`,
  
  joins: {
    Customers: {
      sql: \`\${CUBE}."customerId" = \${Customers}.id\`,
      relationship: \`belongsTo\`
    }
  },
  
  measures: {
    count: {
      type: \`count\`,
      drillMembers: [id]
    },
    
    amount: {
      sql: \`amount\`,
      type: \`sum\`
    }
  },
  
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primaryKey: true
    }
  }
});
`
      },
      {
        fileName: 'Customers.js',
        content: `cube(\`Customers\`, {
  sql: \`SELECT * FROM public.customers\`,
  
  joins: {
    Accounts: {
      sql: \`\${CUBE}."accountId" = \${Accounts}.id\`,
      relationship: \`belongsTo\`
    }
  },
  
  measures: {
    count: {
      type: \`count\`,
      drillMembers: [id, name]
    },
    
    visitCount: {
      sql: \`visit_count\`,
      type: \`sum\`
    }
  },
  
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primaryKey: true
    },
    
    name: {
      sql: \`name\`,
      type: \`string\`
    }
  }
});
`
      },
      {
        fileName: 'Accounts.js',
        content: `cube(\`Accounts\`, {
  sql: \`SELECT * FROM public.accounts\`,
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: \`count\`,
      drillMembers: [id, username]
    },
    
    failurecount: {
      sql: \`\${CUBE}."failureCount"\`,
      type: \`sum\`
    }
  },
  
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primaryKey: true
    },
    
    username: {
      sql: \`username\`,
      type: \`string\`
    },
    
    password: {
      sql: \`password\`,
      type: \`string\`
    }
  }
});
`
      }
    ]);
  });
});
