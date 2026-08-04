/* eslint-disable no-restricted-syntax */
import { RedshiftQuery } from '../../src/adapter/RedshiftQuery';

describe('RedshiftQuery', () => {
  describe('subtractInterval', () => {
    it('does not double-negate a negative interval value into a SQL line comment', () => {
      // e.g. a custom granularity `offset: -1 day` reaches subtractInterval with
      // intervalValue = -1; the result must not contain `--`, which Redshift
      // parses as the start of a line comment.
      const sql = RedshiftQuery.prototype.subtractInterval.call({}, 'created_at', '-1 day');

      expect(sql).not.toContain('--');
      expect(sql).toBe('DATEADD(day, 1, created_at)');
    });

    it('negates a positive interval value as before', () => {
      const sql = RedshiftQuery.prototype.subtractInterval.call({}, 'created_at', '1 day');

      expect(sql).toBe('DATEADD(day, -1, created_at)');
    });
  });
});
