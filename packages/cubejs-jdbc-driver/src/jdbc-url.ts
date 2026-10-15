/**
 * The `;`-delimited JDBC URL flavour (`jdbc:<engine>://<host>[:<port>];Key=Value;…`) used by
 * Databricks, Spark, SQL Server and friends. Those drivers lowercase parameter names and split each
 * entry on its first `=` without trimming, so these helpers do the same. WHATWG `URL` can not parse it.
 */

const URL_DELIMITER = ';';
const PAIR_DELIMITER = '=';

export type JdbcUrlParam = {
  key: string,
  value: string,
  /** Original text of the entry, so params nobody touched are re-emitted byte for byte. */
  raw: string,
};

export class ParsedJdbcUrl {
  public constructor(
    public readonly base: string,
    public readonly params: JdbcUrlParam[],
  ) {}

  public get(name: string): JdbcUrlParam | undefined {
    return this.getAll(name)[0];
  }

  /**
   * A JDBC URL may repeat a parameter, and which entry the driver honours is driver-specific, so a
   * caller that cares about a value has to look at all of them.
   */
  public getAll(name: string): JdbcUrlParam[] {
    const wanted = name.toLowerCase();

    return this.params.filter(({ key }) => key.toLowerCase() === wanted);
  }

  public without(names: string[]): ParsedJdbcUrl {
    const removed = names.map(name => name.toLowerCase());

    return new ParsedJdbcUrl(this.base, this.params.filter(({ key }) => !removed.includes(key.toLowerCase())));
  }

  public toString(): string {
    return [this.base, ...this.params.map(({ raw }) => raw)].join(URL_DELIMITER);
  }
}

export function parseJdbcUrl(jdbcUrl: string): ParsedJdbcUrl {
  const [base, ...parts] = jdbcUrl.split(URL_DELIMITER);

  return new ParsedJdbcUrl(base, parts.map((raw) => {
    const delimiterIndex = raw.indexOf(PAIR_DELIMITER);

    return delimiterIndex >= 0
      ? { key: raw.slice(0, delimiterIndex), value: raw.slice(delimiterIndex + 1), raw }
      : { key: raw, value: '', raw };
  }));
}
