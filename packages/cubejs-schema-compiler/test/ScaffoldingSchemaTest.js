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
        }]
      }
    }, driver);
    const schemaForTables = schema.generateForTables(['public.orders', 'public.customers']);
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
        }]
      }
    }, driver);
    template.generateFilesByTableNames(['public.orders', 'public.customers']).should.be.deepEqual([
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
      }
    ]);
  });
});
