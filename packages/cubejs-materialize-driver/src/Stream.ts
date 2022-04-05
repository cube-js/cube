import { Readable } from 'stream';
import { PoolClient } from 'pg';
// eslint-disable-next-line import/no-extraneous-dependencies
import { TypeId, TypeFormat } from 'pg-types';

/**
 * Thanks to Petros Angelatos
 * https://gist.github.com/petrosagg/804e5f009dee1cb8af688654ba396258
 * This class reads from a cursor in PostgreSQL
 */
export default class Stream extends Readable {
  private conn: PoolClient;

  private cursorId: string;

  private pendingReads: number;

  private currentRows: Array<any>;

  private BreakException = {};

  private typeParser: any;

  public constructor(conn: PoolClient, cursorId: string, highWaterMark: number | undefined, typeParser: (dataTypeID: TypeId, format: TypeFormat | undefined) => any) {
    super({
      highWaterMark,
      objectMode: true,
    });
    this.conn = conn;
    this.typeParser = typeParser;
    this.cursorId = cursorId;
    this.pendingReads = 0;
    this.currentRows = [];
  }

  /**
   * Readable method to fetch data
   * @param n
   */
  public _read(n: number): void {
    if (this.pendingReads <= 0) {
      this.conn
        .query({
          text: `FETCH ${n} ${this.cursorId} WITH (TIMEOUT='1s');`,
          values: [],
          types: { getTypeParser: this.typeParser }
        })
        .then(({ rows }) => {
          /**
           * Process data
           */
          this.process(rows);
        })
        .catch(this.catchClientErr);
    } else {
      /**
       * Process any additional rows
       */
      this.currentRows = this.currentRows.slice(
        this.currentRows.length - this.pendingReads,
        this.currentRows.length
      );
      try {
        this.currentRows.forEach((row) => {
          this.pendingReads -= 1;
          const backPressure = !this.push(row);
          if (backPressure) {
            throw this.BreakException;
          }
        });
      } catch (e) {
        if (e !== this.BreakException) throw e;
      }
    }
  }

  /**
   * Capture any error while fetching results
   * @param clientReasonErr
   */
  private catchClientErr(clientReasonErr: any) {
    this.destroy(clientReasonErr);
  }

  /**
   * Process and push rows
   * @param rows
   */
  private process(rows: Array<any>): void {
    try {
      rows.forEach((row) => {
        this.pendingReads -= 1;
        const backPressure = !this.push(row);
        if (backPressure) {
          throw this.BreakException;
        }
      });

      /**
       * If there is no results from the fetch finish pipe
       */
      if (rows.length === 0) {
        this.push(null);
      }
    } catch (e) {
      if (e !== this.BreakException) throw e;
    }
  }
}
