const chrono = require('chrono-node');
const moment = require('moment');
const UserError = require('./UserError');

module.exports = (dateString) => {
  let momentRange;
  dateString = dateString.toLowerCase();
  if (dateString.match(/(this|last)\s+(day|week|month|year|quarter|hour|minute|second)/)) {
    const match = dateString.match(/(this|last)\s+(day|week|month|year|quarter|hour|minute|second)/);
    let start = moment();
    let end = moment();
    if (match[1] === 'last') {
      start = start.add(-1, match[2]);
      end = end.add(-1, match[2]);
    }
    const span = match[2] === 'week' ? 'isoWeek' : match[2];
    momentRange = [start.startOf(span), end.endOf(span)];
  } else if (dateString.match(/last\s+(\d+)\s+(day|week|month|year|quarter|hour|minute|second)/)) {
    const match = dateString.match(/last\s+(\d+)\s+(day|week|month|year|quarter|hour|minute|second)/);
    const span = match[2] === 'week' ? 'isoWeek' : match[2];
    momentRange = [
      moment().add(-parseInt(match[1], 10) - 1, match[2]).startOf(span),
      moment().add(-1, match[2]).endOf(span)
    ];
  } else if (dateString.match(/today/)) {
    momentRange = [moment().startOf('day'), moment().endOf('day')];
  } else if (dateString.match(/yesterday/)) {
    const yesterday = moment().add(-1, 'day');
    momentRange = [yesterday.startOf('day'), yesterday.endOf('day')];
  } else {
    const results = chrono.parse(dateString);
    if (!results) {
      throw new UserError(`Can't parse date: '${dateString}'`);
    }
    momentRange = results[0].end ? [
      results[0].start.moment(),
      results[0].end.moment()
    ] : [
      results[0].start.moment(),
      results[0].start.moment()
    ];
  }
  return momentRange.map(d => d.format(moment.HTML5_FMT.DATETIME_LOCAL_MS));
};
