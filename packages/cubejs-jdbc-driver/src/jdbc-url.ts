/**
 * The `;`-delimited JDBC URL flavour used by Databricks, Spark, SQL Server and friends:
 * `jdbc:<engine>://<host>[:<port>];Key=Value;Key=Value`.
 *
 * Those drivers lowercase parameter names and split each entry on its *first* `=` without trimming,
 * so these helpers do the same — a padded name really is a different parameter to them.
 *
 * WHATWG `URL` is no help: `jdbc:` is a non-special scheme, so the whole URL lands in `pathname`,
 * and the `;` after the port makes the inner `<engine>://…` invalid outright.
 */

const URL_DELIMITER = ';';
const PAIR_DELIMITER = '=';

export type JdbcUrlParam = {
  key: string,
  value: string,
  /** Original text of the entry, so params nobody touched are re-emitted byte for byte. */
  raw: string,
};

export type ParsedJdbcUrl = {
  /** `jdbc:<engine>://<host>[:<port>]`. */
  base: string,
  params: JdbcUrlParam[],
};

export function parseJdbcUrl(jdbcUrl: string): ParsedJdbcUrl {
  const [base, ...parts] = jdbcUrl.split(URL_DELIMITER);

  return {
    base,
    params: parts.map((raw) => {
      const delimiterIndex = raw.indexOf(PAIR_DELIMITER);

      return delimiterIndex >= 0
        ? { key: raw.slice(0, delimiterIndex), value: raw.slice(delimiterIndex + 1), raw }
        : { key: raw, value: '', raw };
    }),
  };
}

export function formatJdbcUrl({ base, params }: ParsedJdbcUrl): string {
  return [base, ...params.map(({ raw }) => raw)].join(URL_DELIMITER);
}

export function findJdbcUrlParam({ params }: ParsedJdbcUrl, name: string): JdbcUrlParam | undefined {
  return params.find(({ key }) => key.toLowerCase() === name.toLowerCase());
}

export function removeJdbcUrlParams(parsed: ParsedJdbcUrl, names: string[]): ParsedJdbcUrl {
  const removed = names.map(name => name.toLowerCase());

  return {
    ...parsed,
    params: parsed.params.filter(({ key }) => !removed.includes(key.toLowerCase())),
  };
}
