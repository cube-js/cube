const R = require('ramda');
const inflection = require('inflection');

class Funnels {
  constructor(cubeFactory, compiler) {
    this.cubeFactory = cubeFactory;
    this.compiler = compiler;
  }

  // TODO check timeToConvert is absent on first step
  // TODO name can be a title
  eventFunnel(funnelDefinition) {
    if (!funnelDefinition.userId || !funnelDefinition.userId.sql) {
      throw new Error(`userId.sql is not defined`); // TODO schema check
    }
    if (!funnelDefinition.time || !funnelDefinition.time.sql) {
      throw new Error(`time.sql is not defined`); // TODO schema check
    }
    if (!funnelDefinition.steps || !funnelDefinition.steps.length) {
      throw new Error(`steps are not defined`); // TODO schema check
    }

    return this.cubeFactory({
      sql: () => {
        const eventJoin =
          funnelDefinition.steps.map((s, i) => this.eventCubeJoin(funnelDefinition, s, funnelDefinition.steps[i - 1]));
        const userIdColumnsAndTime =
          funnelDefinition.steps.map(s => `${this.eventsTableName(s)}.user_id ${this.stepUserIdColumnName(s)}`)
            .concat([`${this.eventsTableName(funnelDefinition.steps[0])}.t`]).join(",\n");
        return `WITH joined_events AS (
    select
    ${userIdColumnsAndTime}
    FROM
${eventJoin.join("\nLEFT JOIN\n")}
	)
	select user_id, first_step_user_id, step, max(t) t from (
  	${funnelDefinition.steps.map(s => this.stepSegmentSelect(funnelDefinition, s)).join("\nUNION ALL\n")}
	) as event_steps GROUP BY 1, 2, 3`
      },
      measures: {
        conversions: {
          sql: () => `user_id`,
          type: `count`
        },
        firstStepConversions: {
          sql: () => `first_step_user_id`,
          type: `count`,
          shown: false
        },
        conversionsPercent: {
          sql: (conversions, firstStepConversions) => `CASE WHEN ${firstStepConversions} > 0 THEN 100.0 * ${conversions} / ${firstStepConversions} ELSE NULL END`,
          type: `number`,
          format: `percent`
        }
      },

      dimensions: {
        id: {
          sql: (time, step) => `first_step_user_id || ${time} || ${step}`,
          type: `string`,
          primaryKey: true
        },
        userId: {
          sql: () => `user_id`,
          type: `string`,
          shown: false
        },
        firstStepUserId: {
          sql: () => `first_step_user_id`,
          type: `string`,
          shown: false
        },
        time: {
          sql: () => `t`,
          type: `time`
        },
        step: {
          sql: () => `step`,
          type: `string`
        }
      }
    });
  }

  eventCubeJoin(funnelDefinition, step, prevStep) {
    const sql = this.compiler.contextQuery().evaluateSql(null, (step.eventsCube || step.eventsView || step.eventsTable).sql);
    const fromSql = (sql.indexOf('select') !== -1 ? `(${sql}) e` : sql);
    const timeToConvertCondition =
      step.timeToConvert ?
        ` AND ${this.compiler.contextQuery().convertTz(`${this.eventsTableName(step)}.t`)} <= ${this.compiler.contextQuery().addInterval(this.compiler.contextQuery().convertTz(`${this.eventsTableName(prevStep)}.t`), step.timeToConvert)}` :
        '';
    const joinSql =
      prevStep ?
        ` ON ${this.eventsTableName(prevStep)}.${prevStep.nextStepUserId ? 'next_join_user_id' : 'user_id'} = ${this.eventsTableName(step)}.user_id AND ${this.eventsTableName(step)}.t >= ${this.eventsTableName(prevStep)}.t${timeToConvertCondition}` :
        '';
    const nextJoin =
      step.nextStepUserId ? `, ${this.compiler.contextQuery().evaluateSql(null, step.nextStepUserId.sql)} next_join_user_id` : '';
    return `(select ${this.compiler.contextQuery().evaluateSql(null, (step.userId || funnelDefinition.userId).sql)} user_id${nextJoin}, ${this.compiler.contextQuery().evaluateSql(null, (step.time || funnelDefinition.time).sql)} t from ${fromSql}) ${this.eventsTableName(step)}${joinSql}`;
  }

  eventsTableName(step) {
    return `${inflection.underscore(step.name)}_events`;
  }

  stepUserIdColumnName(step) {
    return `${inflection.underscore(step.name)}_user_id`;
  }

  stepSegmentSelect(funnelDefinition, step) {
    return `SELECT ${this.stepUserIdColumnName(step)} user_id, ${this.stepUserIdColumnName(funnelDefinition.steps[0])} first_step_user_id, t, '${inflection.titleize(step.name)}' step FROM joined_events`;
  }
}

module.exports = Funnels;
