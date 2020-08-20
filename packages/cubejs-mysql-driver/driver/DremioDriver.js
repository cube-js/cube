const axios = require('axios');
const BaseDriver = require('@cubejs-backend/query-orchestrator/driver/BaseDriver');
const DremioQuery = require('./DremioQuery');

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
    await this.getToken() 
    return true
  }

  async getToken() { 
    // @todo Check token expiration time
    const {data} = await axios.post(`${this.config.url}/apiv2/login`, {
      "userName": this.config.user,
      "password": this.config.password
    })
    
    this.authToken = `_dremio${data.token}`  
    return this.authToken
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
 
    values.forEach((element, index) => {
      let str = element.replace("\\", "\\\\")
      .replace("\'", "\\\'")
      .replace("\"", "\\\"")
      .replace("\n", "\\\n")
      .replace("\r", "\\\r")
      .replace("\x00", "\\\x00")
      .replace("\x1a", "\\\x1a");
      query = query.replace('?', `'${str}'`)
    })

    await this.getToken();
    const jobId = await this.executeQuery(query)

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
}

module.exports = DremioDriver;
