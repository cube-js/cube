import moment from 'moment';

export
/**
 * Transform cpecified `value` with specified `type` to the network
 * protocol type.
 */
function transformValue(value: any, type: string) {
  // TODO: support for max time
  if (value && (type === 'time' || value instanceof Date)) {
    return (
      value instanceof Date
        ? moment(value)
        : moment.utc(value)
    ).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
  }
  // TODO: move to sql adapter
  return value && value.value ? value.value : value;
}
