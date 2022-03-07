import { BaseFilter, BaseQuery, ParamAllocator } from '@cubejs-backend/schema-compiler';

const GRANULARITY_TO_INTERVAL: Record<string, string> = {
  second: 's',
  minute: 'm',
  hour: 'h',
  day: 'd',
  month: 'M',
  year: 'Y'
};

class QuestParamAllocator extends ParamAllocator {
  public paramPlaceHolder(paramIndex: number) {
    return `$${paramIndex + 1}`;
  }
}

class QuestFilter extends BaseFilter {
  public orIsNullCheck(column: string, not: string): string {
    return `${this.shouldAddOrIsNull(not) ? ` OR ${column} = NULL` : ''}`;
  }

  public setWhere(column: string): string {
    return `${column} != NULL`;
  }

  public notSetWhere(column: string): string {
    return `${column} = NULL`;
  }
}

export class QuestQuery extends BaseQuery {
  public newFilter(filter: any) {
    return new QuestFilter(this, filter);
  }

  public newParamAllocator(): ParamAllocator {
    return new QuestParamAllocator();
  }

  public concatStringsSql(strings: string[]): string {
    return `concat(${strings.join(', ')})`;
  }

  public convertTz(field: string): string {
    return `to_timezone(${field}, '${this.timezone}')`;
  }

  public timeStampCast(value: string) {
    return value;
  }

  public dateTimeCast(value: string) {
    return value;
  }

  public subtractInterval(date: string, interval: string): string {
    const [number, type] = this.parseInterval(interval);
    return `dateadd('${type}', ${-number}, ${date})`;
  }

  public addInterval(date: string, interval: string): string {
    const [number, type] = this.parseInterval(interval);
    return `dateadd('${type}', ${number}, ${date})`;
  }

  public unixTimestampSql(): string {
    // QuestDB's now() function returns epoch timestamp with microsecond granularity.
    return 'now() / 1000000';
  }

  public timeGroupedColumn(granularity: string, dimension: string): string {
    const interval = GRANULARITY_TO_INTERVAL[granularity];
    if (interval === undefined) {
      throw new Error(`${granularity} granularity is not supported`);
    }
    return `timestamp_floor('${GRANULARITY_TO_INTERVAL[granularity]}', ${dimension})`;
  }

  public dimensionsJoinCondition(leftAlias: string, rightAlias: string): string {
    const dimensionAliases = this.dimensionAliasNames();
    if (!dimensionAliases.length) {
      return '1 = 1';
    }
    return dimensionAliases
      .map(alias => `(${leftAlias}.${alias} = ${rightAlias}.${alias} OR (${leftAlias}.${alias} = NULL AND ${rightAlias}.${alias} = NULL))`)
      .join(' AND ');
  }

  public renderSqlMeasure(name: string, evaluateSql: string, symbol: any, cubeName: string, parentMeasure: string): string {
    // QuestDB doesn't support COUNT(column_name) syntax.
    // COUNT() or COUNT(*) should be used instead.

    if (symbol.type === 'count') {
      return 'count(*)';
    }
    return super.renderSqlMeasure(name, evaluateSql, symbol, cubeName, parentMeasure);
  }

  public primaryKeyCount(cubeName: string, distinct: boolean): string {
    if (distinct) {
      const primaryKeySql = this.primaryKeySql(this.cubeEvaluator.primaryKeys[cubeName], cubeName);
      return `count_distinct(${primaryKeySql})`;
    } else {
      return 'count(*)';
    }
  }

  public orderHashToString(hash: any): string | null {
    // QuestDB has partial support for order by index column, so map these to the alias names.
    // So, instead of:
    // SELECT col_a as "a", col_b as "b" FROM tab ORDER BY 2 ASC
    //
    // the query should be:
    // SELECT col_a as "a", col_b as "b" FROM tab ORDER BY "b" ASC

    if (!hash || !hash.id) {
      return null;
    }

    const fieldAlias = this.getFieldAlias(hash.id);

    if (fieldAlias === null) {
      return null;
    }

    const direction = hash.desc ? 'DESC' : 'ASC';
    return `${fieldAlias} ${direction}`;
  }

  private getFieldAlias(id: string): string | null {
    const equalIgnoreCase = (a: any, b: any) => (
      typeof a === 'string' && typeof b === 'string' && a.toUpperCase() === b.toUpperCase()
    );

    let field;

    field = this.dimensionsForSelect().find(
      (d: any) => equalIgnoreCase(d.dimension, id),
    );

    if (field) {
      return field.aliasName();
    }

    field = this.measures.find(
      (d: any) => equalIgnoreCase(d.measure, id) || equalIgnoreCase(d.expressionName, id),
    );

    if (field) {
      return field.aliasName();
    }

    return null;
  }

  public groupByClause(): string {
    // QuestDB doesn't support group by index column, so map these to the alias names.
    // So, instead of:
    // SELECT col_a as "a", count() as "c" FROM tab GROUP BY 1
    //
    // the query should be:
    // SELECT col_a as "a", count() as "c" FROM tab GROUP BY "a"

    if (this.ungrouped) {
      return '';
    }

    const names = this.dimensionAliasNames();
    return names.length ? ` GROUP BY ${names.join(', ')}` : '';
  }
}
