import moment from 'moment-timezone';
import { parse, ParsedComponents } from 'chrono-node';

import { UserError } from './user-error';

const momentFromResult = (result: ParsedComponents, timezone: string): moment.Moment => {
  const dateMoment = moment().tz(timezone);

  dateMoment.set('year', result.get('year') as number);
  dateMoment.set('month', (result.get('month') as number) - 1);
  dateMoment.set('date', result.get('day') as number);
  dateMoment.set('hour', result.get('hour') as number);
  dateMoment.set('minute', result.get('minute') as number);
  dateMoment.set('second', result.get('second') as number);
  dateMoment.set('millisecond', result.get('millisecond') as number);

  return dateMoment;
};

export function dateParser(dateString: string, timezone: string, now: Date = new Date()): string[] {
  let momentRange: moment.Moment[];
  dateString = dateString.toLowerCase();

  if (dateString.match(/(this|last|next)\s+(day|week|month|year|quarter|hour|minute|second)/)) {
    const match = dateString.match(/(this|last|next)\s+(day|week|month|year|quarter|hour|minute|second)/)!;
    const unit = match[2] as moment.unitOfTime.DurationConstructor & moment.unitOfTime.StartOf;
    let start = moment.tz(timezone);
    let end = moment.tz(timezone);
    if (match[1] === 'last') {
      start = start.add(-1, unit);
      end = end.add(-1, unit);
    }
    if (match[1] === 'next') {
      start = start.add(1, unit);
      end = end.add(1, unit);
    }

    const span = unit === 'week' ? 'isoWeek' : unit;
    momentRange = [start.startOf(span), end.endOf(span)];
  } else if (dateString.match(/(last|next)\s+(\d+)\s+(day|week|month|year|quarter|hour|minute|second)/)) {
    const match = dateString.match(/(last|next)\s+(\d+)\s+(day|week|month|year|quarter|hour|minute|second)/)!;
    const unit = match[3] as moment.unitOfTime.DurationConstructor & moment.unitOfTime.StartOf;

    let start = moment.tz(timezone);
    let end = moment.tz(timezone);
    if (match[1] === 'last') {
      start = start.add(-parseInt(match[2], 10), unit);
      end = end.add(-1, unit);
    }
    if (match[1] === 'next') {
      start = start.add(1, unit);
      end = end.add(parseInt(match[2], 10), unit);
    }

    const span = unit === 'week' ? 'isoWeek' : unit;
    momentRange = [start.startOf(span), end.endOf(span)];
  } else if (dateString.match(/today/)) {
    momentRange = [moment.tz(timezone).startOf('day'), moment.tz(timezone).endOf('day')];
  } else if (dateString.match(/yesterday/)) {
    momentRange = [
      moment.tz(timezone).startOf('day').add(-1, 'day'),
      moment.tz(timezone).endOf('day').add(-1, 'day')
    ];
  } else if (dateString.match(/tomorrow/)) {
    momentRange = [
      moment.tz(timezone).startOf('day').add(1, 'day'),
      moment.tz(timezone).endOf('day').add(1, 'day')
    ];
  } else if (dateString.match(/^from (.*) to (.*)$/)) {
    let [, from, to] = dateString.match(/^from(.{0,50})to(.{0,50})$/)!;
    from = from.trim();
    to = to.trim();

    const current = moment(now).tz(timezone);
    const fromResults = parse(from.trim(), new Date(current.format(moment.HTML5_FMT.DATETIME_LOCAL_MS)));
    const toResults = parse(to.trim(), new Date(current.format(moment.HTML5_FMT.DATETIME_LOCAL_MS)));

    if (!Array.isArray(fromResults) || !fromResults.length) {
      throw new UserError(`Can't parse date: '${from}'`);
    }

    if (!Array.isArray(toResults) || !toResults.length) {
      throw new UserError(`Can't parse date: '${to}'`);
    }

    const exactGranularity: moment.unitOfTime.StartOf = (['second', 'minute', 'hour'] as const).find(g => dateString.indexOf(g) !== -1) || 'day';
    momentRange = [
      momentFromResult(fromResults[0].start, timezone),
      momentFromResult(toResults[0].start, timezone)
    ];

    momentRange = [momentRange[0].startOf(exactGranularity), momentRange[1].endOf(exactGranularity)];
  } else {
    const current = moment(now).tz(timezone);
    const results = parse(dateString, new Date(current.format(moment.HTML5_FMT.DATETIME_LOCAL_MS)));

    if (!results?.length) {
      throw new UserError(`Can't parse date: '${dateString}'`);
    }

    const exactGranularity: moment.unitOfTime.StartOf = (['second', 'minute', 'hour'] as const).find(g => dateString.indexOf(g) !== -1) || 'day';
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
}
