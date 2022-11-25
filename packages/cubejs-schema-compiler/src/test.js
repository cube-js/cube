const y = require('js-yaml');

// console.log('>>>', y.dump({
//   sql: 'world',
//   preAggregations: [
//     // hello
//   ],
//   measures: [
//     { name: 'count',
//       type: 'count' }
//   ]
// }));

class ValueWithComments {
  constructor(value, comments = []) {
    this.value = value;
    this.comments = comments;
  }
}

class MemberReference {
  constructor(member) {
    this.member = member;
  }
  
  toString() {
    return this.member;
  }
}

// console.log('>>>', new MemberReference('you').toString());

const q = {
  // sql: new ValueWithComments('select * from', [
  //   'preAggregations:',
  //   'Pre-Aggregations definitions go here',
  //   'Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started',
  // ]),
  // nullish: new ValueWithComments(),
  // preAggregations: new ValueWithComments('select * from', ['hello', 'world']),
  // preAggregations: {
  //   simple: {
  //     timeDimension: 'CUBE.orderDate',
  //     granularity: 'month',
  //   }
  // },
  measures: {
    hello: {
      title: 'Hui',
      type: 'count',
      drillMembers: [
        new MemberReference('amount'),
        new MemberReference('country'),
      ],
    },
    amount: {
      title: 'sdf',
      type: 'number',
      sql: ['sdf', 'ff'],
      drillMembers: undefined
    },
  },
  dimensions: {
    status: {
      type: 'string',
      case: {
        when: [
          {
            sql: '${CUBE}.status = 1',
            label: 'Approved',
          },
          {
            sql: '${CUBE}.status = 2',
            label: 'Canceled',
          },
        ],
        else: { label: 'Unknown' },
      },
    },
    created_at: {
      type: 'time',
      sql: 'created_at',
    },
  },
};

// q = {
//   title: 'Cloud API Failed Requests',

//   sql: `
//     SELECT
//       id,
//       timestamp,
//       event,
//     WHERE queries_prop.value != '0'
//   `,

//   joins: {
//     CloudOrganizations: {
//       relationship: 'belongsTo',
//       sql: '${CUBE.tenantId} = ${CloudOrganizations}.id'
//     }
//   },

//   measures: {
//     rateErrorQueries: {
//       type: 'number',
//       sql: '(${CUBE.countErrorQueries} / ${CUBE.countAllQueries}) * 100',
//     },

//     countErrorQueries: {
//       type: 'sum',
//       sql: 'CAST(queries as INT)',
//       filters: [
//         { sql: '${CUBE}.event = \'cube_cloud_error_while_querying\'' }
//       ]
//     },

//     countAllQueries: {
//       type: 'sum',
//       sql: 'CAST(queries as INT)',
//     },

//     countSuccessQueries: {
//       type: 'sum',
//       sql: 'CAST(queries as INT)',
//       filters: [
//         { sql: '${CUBE}.event = \'cube_cloud_load_request_success\'' }
//       ]
//     },
//   },

//   dimensions: {
//     id: {
//       sql: 'id',
//       type: 'string',
//       primaryKey: true
//     },

//     timestamp: {
//       sql: 'timestamp',
//       type: 'time'
//     },

//     tenantId: {
//       sql: 'CAST(cloud_tenant_id as INT)',
//       type: 'number'
//     },

//     deploymentId: {
//       sql: 'cloud_deployment_id',
//       type: 'string'
//     },

//     apiType: {
//       sql: 'api_type',
//       type: 'string'
//     },

//     dbType: {
//       sql: 'db_type',
//       type: 'string'
//     },

//     isPaidOrganization: {
//       sql: '${CloudOrganizations.isPaid}',
//       type: 'boolean'
//     },

//     isProductionOrganization: {
//       sql: '${CloudOrganizations.isProduction}',
//       type: 'boolean'
//     },

//     organizationName: {
//       sql: '${CloudOrganizations.name}',
//       type: 'string'
//     }
//   }
// };

let x = 100;

function isPlainObject(value) {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  return Object.getPrototypeOf(value) === Object.getPrototypeOf({});
}

// console.log('>>>!!!', isPlainObj([]), isPlainObj(new MemberReference('ff')));

function render(value, level = 0, parent) {
  x--;
  if (x <= 0) throw new Error('fuck');

  const indent = Array(level * 2)
    .fill(0)
    .reduce((memo) => `${memo}.`, '');

  if (value instanceof MemberReference) {
    return value.member;
  } else if (value instanceof ValueWithComments) {
    const comments = `\n${value.comments
      .map((comment) => `${indent}# ${comment}`)
      .join('\n')}`;

    return value.value ? `${render(value.value)}${comments}` : comments;
  } else if (Array.isArray(value)) {
    if (value.every((v) => typeof v !== 'object' || v instanceof MemberReference)) {
      return ` [${value.map(render).join(', ')}]\n`;
    }
    
    return `\n${value
      .map((v) => `${indent}- ${render(v, level + 1, value)}`)
      .join('\n')}`;
  } else if (typeof value === 'object') {
    if (parent) {
      return `${!Array.isArray(parent) ? '\n' : ''}${Object.entries(value)
        .map(
          ([k, v], index) => `${
            Array.isArray(parent) && index === 0 ? '' : `${indent}`
          }${k}:${render(v, level + 1, value)}`
        )
        .join('\n')}`;
    }

    const content = Object.keys(value)
      .map((k) => {
        if (!isPlainObject(value[k])) {
          return `${indent}${k}:${render(value[k], level + 1, value)}`;
        }
        // todo: check which keys should be converted to Array
        return `${indent}${k}:${render(
          Object.entries(value[k] || {}).map(([ok, ov]) => ({
            name: ok,
            ...ov,
          })),
          level + 1,
          value
        )}`;
      })
      .join('\n');

    // return `\n${indent}${content}`;
    return `\n${content}`;
  }

  return `${Array.isArray(parent) ? '' : ' '}${value}`;
}

// measures
//

console.log('>>>', JSON.stringify(q, null, 2));
// console.log(y.dump(JSON.parse(JSON.stringify(q))));
// console.log(render(q, 1));
// console.log(`cubes:\n  - name: hui${render(q, 0)})\n`);
