import R from 'ramda';
import {
  BaseFilter,
  BaseQuery,
  ParamAllocator
} from '@cubejs-backend/schema-compiler';

const GRANULARITY_TO_INTERVAL: Record<string, string> = {
  second: 's',
  minute: 'm',
  hour: 'h',
  day: 'd',
  week: 'w',
  month: 'M',
  year: 'y'
};

// QuestDB dateadd/datediff take a single-character period unit (e.g. 'd', 'M'),
// not the full word Cube's parseInterval yields ('day', 'month', …). QuestDB has
// no quarter unit, so it is expressed as 3 months.
const INTERVAL_TO_QUEST_DATE_UNIT: Record<string, { unit: string, factor: number }> = {
  second: { unit: 's', factor: 1 },
  minute: { unit: 'm', factor: 1 },
  hour: { unit: 'h', factor: 1 },
  day: { unit: 'd', factor: 1 },
  week: { unit: 'w', factor: 1 },
  month: { unit: 'M', factor: 1 },
  quarter: { unit: 'M', factor: 3 },
  year: { unit: 'y', factor: 1 },
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
  public newFilter(filter: any): BaseFilter {
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
    const { unit, factor } = INTERVAL_TO_QUEST_DATE_UNIT[type];

    return `dateadd('${unit}', ${-number * factor}, ${date})`;
  }

  public addInterval(date: string, interval: string): string {
    const [number, type] = this.parseInterval(interval);
    const { unit, factor } = INTERVAL_TO_QUEST_DATE_UNIT[type];

    return `dateadd('${unit}', ${number * factor}, ${date})`;
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

  public dateBin(interval: string, source: string, origin: string): string {
    const { stride, unit, count } = this.questFloorStride(interval);
    const shiftedOrigin = `dateadd('${unit}', ${-1000 * count}, cast('${origin}' as timestamp))`;
    return `timestamp_floor('${stride}', ${source}, ${shiftedOrigin})`;
  }

  private questFloorStride(interval: string): { stride: string, unit: string, count: number } {
    const parts = [...interval.matchAll(/(\d+)\s+(second|minute|hour|day|week|month|quarter|year)s?/g)];
    if (parts.length !== 1) {
      throw new Error(`QuestDB supports only single-unit custom granularities, got: ${interval}`);
    }

    const [, num, type] = parts[0];
    const { unit, factor } = INTERVAL_TO_QUEST_DATE_UNIT[type];

    const count = parseInt(num, 10) * factor;
    return { stride: `${count}${unit}`, unit, count };
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

  public baseHaving(query: string, filters: BaseFilter[]) {
    // QuestDB doesn't support HAVING syntax.
    // `( <query> ) WHERE <filter>` should be used instead.

    if (filters.length > 0) {
      let filter = filters.map(t => t.filterToWhere()).filter(R.identity).map(f => `(${f})`).join(' AND ');
      // Replace measures with their aliases in the filter.
      this.measures.forEach((m) => {
        filter = filter.replace(m.measureSql(), m.aliasName());
      });
      return `SELECT * FROM (${query}) WHERE ${filter}`;
    }
    return query;
  }

  public renderSqlMeasure(name: string, evaluateSql: string, symbol: any, cubeName: string, parentMeasure: string, orderBySql: string[]): string {
    // QuestDB doesn't support COUNT(column_name) syntax.
    // COUNT() or COUNT(*) should be used instead.

    if (symbol.type === 'count') {
      return 'count(*)';
    }
    return super.renderSqlMeasure(name, evaluateSql, symbol, cubeName, parentMeasure, orderBySql);
  }

  public primaryKeyCount(cubeName: string, distinct: boolean): string {
    const primaryKeys: string[] = this.cubeEvaluator.primaryKeys[cubeName];
    const primaryKeySql = primaryKeys.length > 1 ?
      this.concatStringsSql(primaryKeys.map((pk) => this.castToString(this.primaryKeySql(pk, cubeName)))) :
      this.primaryKeySql(primaryKeys[0], cubeName);
    if (distinct) {
      return `count_distinct(${primaryKeySql})`;
    } else {
      return 'count(*)';
    }
  }

  public orderHashToString(hash: { id: string, desc: boolean }): string | null {
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

  public countDistinctApprox(sql: string): string {
    return `approx_count_distinct(${sql})`;
  }

  // QuestDB has no standalone OFFSET keyword; it uses `LIMIT lo, hi` (skip `lo`
  // rows, return up to position `hi`).
  public limitOffsetClause(limit: string | number | null, offset: string | number | null): string {
    const o = offset != null ? parseInt(`${offset}`, 10) : null;
    const l = limit != null ? parseInt(`${limit}`, 10) : null;
    if (o != null && l != null) {
      return ` LIMIT ${o}, ${o + l}`;
    }

    if (o != null) {
      return ` LIMIT ${o}, 2147483647`;
    }

    if (l != null) {
      return ` LIMIT ${l}`;
    }

    return '';
  }

  public sqlTemplates() {
    const templates = super.sqlTemplates();
    // eslint-disable-next-line no-template-curly-in-string
    templates.params.param = '${{ param_index + 1 }}';

    // QuestDB does not support the `NULLS FIRST/LAST` ordering keywords.
    templates.expressions.sort = '{{ expr }} {% if asc %}ASC{% else %}DESC{% endif %}';
    templates.expressions.order_by = '{% if index %}{{ index }}{% else %}{{ expr }}{% endif %} {% if asc %}ASC{% else %}DESC{% endif %}';

    templates.statements.time_series_select = 'SELECT cast(dates.f as timestamp) date_from, cast(dates.t as timestamp) date_to \n' +
      'FROM (\n' +
      '{% for time_item in seria %}' +
      '    select \'{{ time_item[0] }}\' f, \'{{ time_item[1] }}\' t \n' +
      '{% if not loop.last %} UNION ALL\n{% endif %}' +
      '{% endfor %}' +
      ') AS dates';

    // QuestDB uses `LIMIT lo, hi` instead of `LIMIT n OFFSET m` (there is no
    // standalone OFFSET keyword). The only change from the base SELECT template
    // is the limit/offset tail: hi = offset + limit, or a large sentinel when
    // only an offset is given.
    templates.statements.select = '{% if ctes %} WITH {% if recursive %}RECURSIVE {% endif %}\n' +
      '{{ ctes | join(\',\n\') }}\n' +
      '{% endif %}' +
      'SELECT {% if distinct %}DISTINCT {% endif %}' +
      '{{ select_concat | map(attribute=\'aliased\') | join(\', \') }} {% if from %}\n' +
      'FROM (\n' +
      '{{ from | indent(2, true) }}\n' +
      ') AS {{ from_alias }}{% elif from_prepared %}\n' +
      'FROM {{ from_prepared }}' +
      '{% endif %}' +
      '{% for join in joins %}\n{{ join }}{% endfor %}' +
      '{% if filter %}\nWHERE {{ filter }}{% endif %}' +
      '{% if group_by %}\nGROUP BY {{ group_by }}{% endif %}' +
      '{% if having %}\nHAVING {{ having }}{% endif %}' +
      '{% if order_by %}\nORDER BY {{ order_by | map(attribute=\'expr\') | join(\', \') }}{% endif %}' +
      '{% if offset is not none and limit is not none %}\nLIMIT {{ offset }}, {{ (offset | int) + (limit | int) }}' +
      '{% elif offset is not none %}\nLIMIT {{ offset }}, 2147483647' +
      '{% elif limit is not none %}\nLIMIT {{ limit }}{% endif %}';

    return templates;
  }
}
