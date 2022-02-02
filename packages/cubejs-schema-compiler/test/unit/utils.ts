interface CreateCubeSchemaOptions {
  name: string,
  refreshKey?: string,
  preAggregations?: string,
  alias?: string,
}

export function createCubeSchema({ name, refreshKey = '', preAggregations = '', alias = undefined }: CreateCubeSchemaOptions) {
  return ` 
    cube('${name}', {
        sql: \`
        select * from cards
        \`,

        ${alias ? '' : `        sqlAlias: '${alias}',`}
        
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
