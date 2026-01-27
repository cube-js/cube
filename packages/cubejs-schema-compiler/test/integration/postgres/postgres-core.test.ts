import { dbRunner } from './PostgresDBRunner';

// Core SQL generation, data schema, and expression tests
import './async-module.suite';
import './bucketing.suite';
import './calc-groups.suite';
import './calendars.suite';
import './custom-granularities.suite';
import './dataschema-compiler.suite';
import './member-expression.suite';
import './member-expressions-on-views.suite';
import './multi-fact-join.suite';
import './multi-stage.suite';
import './multiple-join-paths.suite';
import './sql-generation-logic.suite';
import './sql-generation.suite';
import './sub-query-dimensions.suite';
import './yaml-compiler.suite';

afterAll(async () => {
  await dbRunner.tearDown();
});
