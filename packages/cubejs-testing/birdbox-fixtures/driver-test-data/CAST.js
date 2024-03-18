export const DB_CAST = {
  athena: {
    SELECT_PREFIX: '',
    SELECT_SUFFIX: '',
  },
  bigquery: {
    SELECT_PREFIX: '',
    SELECT_SUFFIX: '',
  },
  firebolt: {
    SELECT_PREFIX: '',
    SELECT_SUFFIX: '',
  },
  postgres: {
    SELECT_PREFIX: '',
    SELECT_SUFFIX: '',
  },
  'databricks-jdbc': {
    SELECT_PREFIX: '',
    SELECT_SUFFIX: '',
  },
  questdb: {
    SELECT_PREFIX: 'with tmp_tbl as (\n',
    SELECT_SUFFIX: ')\nselect * from tmp_tbl',
  },
  vertica: {
    SELECT_PREFIX: '',
    SELECT_SUFFIX: '',
  },
};
export const DATE_CAST = {
  athena: {
    DATE_PREFIX: 'date_parse(',
    DATE_SUFFIX: ', \'%Y-%m-%d\')',
  },
  bigquery: {
    DATE_PREFIX: 'parse_date(\'%Y-%m-%d\', ',
    DATE_SUFFIX: ')',
  },
  firebolt: {
    DATE_PREFIX: 'to_date(',
    DATE_SUFFIX: ')',
  },
  postgres: {
    DATE_PREFIX: 'to_date(',
    DATE_SUFFIX: ', \'YYYY-MM-DD\')',
  },
  'databricks-jdbc': {
    DATE_PREFIX: 'to_date(',
    DATE_SUFFIX: ', \'y-M-d\')',
  },
  questdb: {
    DATE_PREFIX: 'to_date(',
    DATE_SUFFIX: ', \'YYYY-MM-DD\')',
  },
  vertica: {
    DATE_PREFIX: 'to_date(',
    DATE_SUFFIX: ', \'YYYY-MM-DD\')',
  },
};
