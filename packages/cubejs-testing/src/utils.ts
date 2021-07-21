import { platform } from 'os';

export function getLocalHostnameByOs() {
  if (platform() === 'win32') {
    return 'docker.for.win.localhost';
  }

  if (platform() === 'darwin') {
    return 'host.docker.internal';
  }

  return 'localhost';
}

interface CreateCubeSchemaOptions {
  name: string,
  refreshKey?: string,
  preAggregations?: string,
}

export function createCubeSchema({ name, refreshKey = '', preAggregations = '' }: CreateCubeSchemaOptions) {
  return ` 
    cube('${name}', {
        sql: \`
        select * from cards
        \`,
        
        ${refreshKey}
   
        measures: {
          count: {
            type: 'count'
          },
          sum: {
            sql: \`amount\`,
            type: \`sum\`
          },
          max: {
            sql: \`amount\`,
            type: \`max\`
          },
          min: {
            sql: \`amount\`,
            type: \`min\`
          }
        },
  
        dimensions: {
          id: {
            type: 'number',
            sql: 'id',
            primaryKey: true
          },
          createdAt: {
            type: 'time',
            sql: 'created_at'
          },
        },
        
        preAggregations: {
            ${preAggregations}
        }
      }) 
  `;
}
