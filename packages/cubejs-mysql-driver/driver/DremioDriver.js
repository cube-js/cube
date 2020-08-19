
const crypto = require('crypto');
const axios = require('axios');
const BaseDriver = require('@cubejs-backend/query-orchestrator/driver/BaseDriver');
const DremioQuery = require('./DremioQuery');


const GenericTypeToMySql = {
  string: 'varchar(255) CHARACTER SET utf8mb4',
  text: 'varchar(255) CHARACTER SET utf8mb4'
};
 

class DremioDriver extends BaseDriver {

  static dialectClass() {
    return DremioQuery;
  }

  constructor(config) {
    super();
     
    this.config = {
      host: process.env.CUBEJS_DB_HOST || 'localhost', 
      port: process.env.CUBEJS_DB_PORT || 9047,
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS, 
      database: process.env.CUBEJS_DB_NAME, 
    };
    
    this.config.url = `http://${this.config.host}:${this.config.port}` 
  }

  async testConnection() {
    try{
      await this.getToken()
    }catch(err){
      return false
    }
    
    return true
  }

  async getToken() { 
    // @todo Check token expiration time
    return new Promise((resolve, reject) => {
      axios.post(`${this.config.url}/apiv2/login`, {
        "userName": this.config.user,
        "password": this.config.password
      })
      .then(response => { 
        this.authToken = `_dremio${response.data.token}` 
        resolve(this.authToken)
      })
      .catch(reject);
    })
  }

  async restDremioQuery(type, url, data) {  
    if(type == 'get')
    {
      return axios[type](`${this.config.url}${url}`, {
        headers: {
          Authorization: `${this.authToken}` 
        }
      })
    }
    return axios[type](`${this.config.url}${url}`, data, {
      headers: {
        Authorization: `${this.authToken}` 
      }
    })
  }

  async getJobStatus(jobId) {  
    return this.restDremioQuery('get', `/api/v3/job/${jobId}`) 
  }

  async getJobResults(jobId, limit = 500, offset = 0) { 
    // @todo Retrieve all values, not maximum 500
    return axios.get(`${this.config.url}/api/v3/job/${jobId}/results?offset=${offset}&limit=${limit}`, {
      headers: {
        Authorization: `${this.authToken}` 
      }
    })
  }

  async sleep(time) {  
    await new Promise((resolve, reject) => setTimeout(resolve, time));
  }

  async executeQuery(sql) {  
    const {data} = await axios.post(`${this.config.url}/api/v3/sql`, { sql }, {
      headers: {
        Authorization: `${this.authToken}` 
      }
    })
    return data.id
  }

  async query(query, values) {
 
    console.log("MY-SQL", query, values)
    // `
    // SELECT 
    //   account_id, ophid, DATE_TRUNC('hour', "timestamp"), sum("value")
    // FROM 
    //   "ib-dl-prometheus-preprod"."metrics-000002".metric
    // WHERE 
    //       name = 'onprem_cdc_accepted_dns_cloud_events'
    //   and account_id = '2002519'
    // GROUP BY DATE_TRUNC('hour', "timestamp"), ophid, account_id
    // `

    
    await this.getToken();
    const jobId = await this.executeQuery(query)

    console.log("MY-authToken", this.authToken)
         

    do{ 
      const {data} = await this.getJobStatus(jobId)

      console.log(data.jobState, jobId)

      if(data.jobState === 'FAILED')
      {
        throw new Error(data.errorMessage) 
      }
      else if(data.jobState === 'CANCELED')
      {
        throw new Error(`Job ${jobId} was been canceled`) 
      }
      else if(data.jobState === 'COMPLETED')
      { 
        let {data} = await this.getJobResults(jobId) 
        console.log(JSON.stringify(data))
        return data.rows
      }

      await this.sleep(1000)
    }while(true);  
  }

  async release() { 
  }

  async tablesSchema() {
    const {data} = await this.restDremioQuery('get', `/api/v3/catalog/by-path/${this.config.database}`)

    let querys = []
    data.children.forEach(element => { 
      let url = element.path.join('/') 
      querys.push(this.restDremioQuery('get', `/api/v3/catalog/by-path/${url}`))
    });
 
    return new Promise((resolve, reject) => { 
      Promise.all(querys).then((results) => { 
        let schema = {}
        for(let i in results){

          let val = results[i].data
          if(val.entityType != 'dataset'){
            // @todo select children recursively.
            continue;
          }

          let fields = {}
          val.fields.forEach(i =>{
            fields[i.name] = { name: i.name, type: 'string'  } // i.type.name
          })

          schema[val.path.join('.')] = fields
        }
        
        console.log(schema)
        resolve(schema)
      })
      .catch(reject); 
    })
  }
  
  setTimeZone(db) {
    return db.execute(`SET time_zone = '${this.config.storeTimezone || '+00:00'}'`, []);
  }

  fromGenericType(columnType) {
    return GenericTypeToMySql[columnType] || super.fromGenericType(columnType);
  }

  loadPreAggregationIntoTable(preAggregationTableName, loadSql, params, tx) {
    if (this.config.loadPreAggregationWithoutMetaLock) {
      return this.cancelCombinator(async saveCancelFn => {
        await saveCancelFn(this.query(`${loadSql} LIMIT 0`, params));
        await saveCancelFn(this.query(loadSql.replace(/^CREATE TABLE (\S+) AS/i, 'INSERT INTO $1'), params));
      });
    }
    return super.loadPreAggregationIntoTable(preAggregationTableName, loadSql, params, tx);
  }

  async downloadQueryResults(query, values) {
    if (!this.config.database) {
      throw new Error(`Default database should be defined to be used for temporary tables during query results downloads`);
    }
    const tableName = crypto.randomBytes(10).toString('hex');
    const columns = await this.withConnection(async db => {
      await this.setTimeZone(db);
      await db.execute(`CREATE TEMPORARY TABLE \`${this.config.database}\`.t_${tableName} AS ${query} LIMIT 0`, values);
      const result = await db.execute(`DESCRIBE \`${this.config.database}\`.t_${tableName}`);
      await db.execute(`DROP TEMPORARY TABLE \`${this.config.database}\`.t_${tableName}`);
      return result;
    });

    const types = columns.map(c => ({ name: c.Field, type: this.toGenericType(c.Type) }));

    return {
      rows: await this.query(query, values),
      types,
    };
  }

  toColumnValue(value, genericType) {
    if (genericType === 'timestamp' && typeof value === 'string') {
      return value && value.replace('Z', '');
    }
    if (genericType === 'boolean' && typeof value === 'string') {
      if (value.toLowerCase() === 'true') {
        return true;
      }
      if (value.toLowerCase() === 'false') {
        return false;
      }
    }
    return super.toColumnValue(value, genericType);
  }

  async uploadTable(table, columns, tableData) {
    if (!tableData.rows) {
      throw new Error(`${this.constructor} driver supports only rows upload`);
    }
    await this.createTable(table, columns);
    try {
      const batchSize = 1000; // TODO make dynamic?
      for (let j = 0; j < Math.ceil(tableData.rows.length / batchSize); j++) {
        const currentBatchSize = Math.min(tableData.rows.length - j * batchSize, batchSize);
        const indexArray = Array.from({ length: currentBatchSize }, (v, i) => i);
        const valueParamPlaceholders =
          indexArray.map(i => `(${columns.map((c, paramIndex) => this.param(paramIndex + i * columns.length)).join(', ')})`).join(', ');
        const params = indexArray.map(i => columns
          .map(c => this.toColumnValue(tableData.rows[i + j * batchSize][c.name], c.type)))
          .reduce((a, b) => a.concat(b), []);

        await this.query(
          `INSERT INTO ${table}
        (${columns.map(c => this.quoteIdentifier(c.name)).join(', ')})
        VALUES ${valueParamPlaceholders}`,
          params
        );
      }
    } catch (e) {
      await this.dropTable(table);
      throw e;
    }
  }
}

module.exports = DremioDriver;
