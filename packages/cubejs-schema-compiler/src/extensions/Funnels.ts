import inflection from 'inflection';
import { AbstractExtension } from './extension.abstract';

type FunnelSql = {
  sql: (...args: any[]) => string;
};

type FunnelStep = {
  name: string;
  eventsCube?: FunnelSql;
  eventsView?: FunnelSql;
  eventsTable?: FunnelSql;
  userId?: FunnelSql;
  time?: FunnelSql;
  nextStepUserId?: FunnelSql;
  timeToConvert?: string;
};

type FunnelDefinition = {
  userId: FunnelSql;
  time: FunnelSql;
  steps: FunnelStep[];
};

export class Funnels extends AbstractExtension {
  // TODO check timeToConvert is absent on first step
  // TODO name can be a title
  public eventFunnel(funnelDefinition: FunnelDefinition) {
    if (!funnelDefinition.userId || !funnelDefinition.userId.sql) {
      throw new Error('userId.sql is not defined'); // TODO schema check
    }

    if (!funnelDefinition.time || !funnelDefinition.time.sql) {
      throw new Error('time.sql is not defined'); // TODO schema check
    }

    if (!funnelDefinition.steps || !funnelDefinition.steps.length) {
      throw new Error('steps are not defined'); // TODO schema check
    }

    return this.cubeFactory({
      sql: () => {
        const eventJoin =
          funnelDefinition.steps.map((s: FunnelStep, i: number) => this.eventCubeJoin(funnelDefinition, s, funnelDefinition.steps[i - 1]));
        const userIdColumnsAndTime =
          funnelDefinition.steps.map((s: FunnelStep) => `${this.eventsTableName(s)}.user_id ${this.stepUserIdColumnName(s)}`)
            .concat([`${this.eventsTableName(funnelDefinition.steps[0])}.t`]).join(',\n');
        return `WITH joined_events AS (
    select
    ${userIdColumnsAndTime}
    FROM
${eventJoin.join('\nLEFT JOIN\n')}
  )
  select user_id, first_step_user_id, step, max(t) t from (
    ${funnelDefinition.steps.map((s: FunnelStep) => this.stepSegmentSelect(funnelDefinition, s)).join('\nUNION ALL\n')}
  ) as event_steps GROUP BY 1, 2, 3`;
      },
      measures: {
        conversions: {
          sql: () => 'user_id',
          type: 'count'
        },
        firstStepConversions: {
          sql: () => 'first_step_user_id',
          type: 'countDistinct',
          shown: false
        },
        conversionsPercent: {
          sql: (conversions: string, firstStepConversions: string) => `CASE WHEN ${firstStepConversions} > 0 THEN 1.0 * ${conversions} / ${firstStepConversions} ELSE NULL END`,
          type: 'number',
          format: 'percent'
        }
      },

      dimensions: {
        id: {
          sql: (time: string, step: string) => `first_step_user_id || ${time} || ${step}`,
          type: 'string',
          primaryKey: true
        },
        userId: {
          sql: () => 'user_id',
          type: 'string',
          shown: false
        },
        firstStepUserId: {
          sql: () => 'first_step_user_id',
          type: 'string',
          shown: false
        },
        time: {
          sql: () => 't',
          type: 'time'
        },
        step: {
          sql: () => 'step',
          type: 'string'
        }
      }
    });
  }

  protected eventCubeJoin(funnelDefinition: FunnelDefinition, step: FunnelStep, prevStep: FunnelStep | undefined) {
    const sql = this.compiler.contextQuery().evaluateSql(
      null,
      (step.eventsCube || step.eventsView || step.eventsTable)!.sql
    );
    const fromSql = (sql.toLowerCase().trim().startsWith('select') ? `(${sql}) e` : sql);
    const timeToConvertCondition =
      step.timeToConvert ?
        ` AND ${this.compiler.contextQuery().convertTz(`${this.eventsTableName(step)}.t`)} <= ${this.compiler.contextQuery().addInterval(this.compiler.contextQuery().convertTz(`${this.eventsTableName(prevStep!)}.t`), step.timeToConvert)}` :
        '';
    const joinSql =
      prevStep ?
        ` ON ${this.eventsTableName(prevStep)}.${prevStep.nextStepUserId ? 'next_join_user_id' : 'user_id'} = ${this.eventsTableName(step)}.user_id AND ${this.eventsTableName(step)}.t >= ${this.eventsTableName(prevStep)}.t${timeToConvertCondition}` :
        '';
    const nextJoin =
      step.nextStepUserId ? `, ${this.compiler.contextQuery().evaluateSql(null, step.nextStepUserId.sql)} next_join_user_id` : '';
    return `(select ${this.compiler.contextQuery().evaluateSql(null, (step.userId || funnelDefinition.userId).sql)} user_id${nextJoin}, ${this.compiler.contextQuery().evaluateSql(null, (step.time || funnelDefinition.time).sql)} t from ${fromSql}) ${this.eventsTableName(step)}${joinSql}`;
  }

  protected eventsTableName(step: FunnelStep) {
    return `${this.inflect(step)}_events`;
  }

  protected stepUserIdColumnName(step: FunnelStep) {
    return `${this.inflect(step)}_user_id`;
  }

  protected inflect(step: FunnelStep) {
    return inflection.underscore(inflection.camelize(step.name.replace(/[^A-Za-z0-9]+/g, '_')));
  }

  protected stepSegmentSelect(funnelDefinition: FunnelDefinition, step: FunnelStep) {
    return `SELECT ${this.stepUserIdColumnName(step)} user_id, ${this.stepUserIdColumnName(funnelDefinition.steps[0])} first_step_user_id, t, '${inflection.titleize(step.name)}' step FROM joined_events`;
  }
}
