const chrono = require('chrono-node');
const moment = require('moment-timezone');
const UserError = require('./UserError');

const momentFromResult = (result, timezone) => {
  const dateMoment = moment().tz(timezone);

  dateMoment.set('year', result.get('year'));
  dateMoment.set('month', result.get('month') - 1);
  dateMoment.set('date', result.get('day'));
  dateMoment.set('hour', result.get('hour'));
  dateMoment.set('minute', result.get('minute'));
  dateMoment.set('second', result.get('second'));
  dateMoment.set('millisecond', result.get('millisecond'));

  return dateMoment;
};

module.exports = (dateString, timezone) => {
  let momentRange;
  dateString = dateString.toLowerCase();
  if (dateString.match(/(this|last)\s+(day|week|month|year|quarter|hour|minute|second)/)) {
    const match = dateString.match(/(this|last)\s+(day|week|month|year|quarter|hour|minute|second)/);
    let start = moment.tz(timezone);
    let end = moment.tz(timezone);
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
      moment.tz(timezone).startOf(span).add(-parseInt(match[1], 10), match[2]),
      moment.tz(timezone).endOf(span).add(-1, match[2])
    ];
  } else if (dateString.match(/today/)) {
    momentRange = [moment.tz(timezone).startOf('day'), moment.tz(timezone).endOf('day')];
  } else if (dateString.match(/yesterday/)) {
    momentRange = [
      moment.tz(timezone).startOf('day').add(-1, 'day'),
      moment.tz(timezone).endOf('day').add(-1, 'day')
    ];
  } else if (dateString.match(/^from (.*) to (.*)$/)) {
    // eslint-disable-next-line no-unused-vars
    const [all, from, to] = dateString.match(/^from (.*) to (.*)$/);
    const fromResults = chrono.parse(from, moment().tz(timezone));
    const toResults = chrono.parse(to, moment().tz(timezone));
    if (!fromResults) {
      throw new UserError(`Can't parse date: '${from}'`);
    }
    if (!toResults) {
      throw new UserError(`Can't parse date: '${to}'`);
    }
    const exactGranularity = ['second', 'minute', 'hour'].find(g => dateString.indexOf(g) !== -1) || 'day';
    momentRange = [
      momentFromResult(fromResults[0].start, timezone),
      momentFromResult(toResults[0].start, timezone)
    ];
    momentRange = [momentRange[0].startOf(exactGranularity), momentRange[1].endOf(exactGranularity)];
  } else {
    const results = chrono.parse(dateString, moment().tz(timezone));
    if (!results) {
      throw new UserError(`Can't parse date: '${dateString}'`);
    }
    const exactGranularity = ['second', 'minute', 'hour'].find(g => dateString.indexOf(g) !== -1) || 'day';
    momentRange = results[0].end ? [
      momentFromResult(results[0].start, timezone),
      momentFromResult(results[0].end, timezone)
    ] : [
      momentFromResult(results[0].start, timezone),
      momentFromResult(results[0].start, timezone)
    ];
    momentRange = [momentRange[0].startOf(exactGranularity), momentRange[1].endOf(exactGranularity)];
  }
  return momentRange.map(d => d.format(moment.HTML5_FMT.DATETIME_LOCAL_MS));
};
