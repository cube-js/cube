import { dbRunner } from './PostgresDBRunner';

// SQL generation and data schema tests
import './async-module.suite';
import './dataschema-compiler.suite';
import './member-expression.suite';
import './member-expressions-on-views.suite';
import './sql-generation-logic.suite';
import './sql-generation.suite';
import './yaml-compiler.suite';

afterAll(async () => {
  await dbRunner.tearDown();
});