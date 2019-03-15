const moment = require('moment-timezone');

const MysqlQuery = require('./MysqlQuery');

class MongoBiQuery extends MysqlQuery {
  convertTz(field) {
    const tz = moment().tz(this.timezone);
    // TODO respect day light saving
    const [hour, minute] = tz.format('Z').split(':');
    const [hourInt, minuteInt] = [parseInt(hour, 10), parseInt(minute, 10) * Math.sign(parseInt(hour, 10))];
    let result = field;
    if (hourInt !== 0) {
      result = `TIMESTAMPADD(HOUR, ${hourInt}, ${result})`;
    }
    if (minuteInt !== 0) {
      result = `TIMESTAMPADD(HOUR, ${minuteInt}, ${result})`;
    }
    return result;
  }

  timeStampCast(value) {
    return `TIMESTAMP(${value})`;
  }
}

module.exports = MongoBiQuery;
