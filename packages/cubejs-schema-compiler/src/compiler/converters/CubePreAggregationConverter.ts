import { parse } from '@babel/parser';
import * as t from '@babel/types';

import { UserError } from '../UserError';

import { AstByCubeName, CubeConverterInterface } from './CubeSchemaConverter';

export type PreAggregationDefinition = {
  cubeName: string;
  preAggregationName: string;
  code: string;
};

export class CubePreAggregationConverter implements CubeConverterInterface {
  public constructor(protected preAggregationDefinition: PreAggregationDefinition) {}

  public convert(astByCubeName: AstByCubeName): void {
    const { cubeName, preAggregationName, code } = this.preAggregationDefinition;
    const { cubeDefinition } = astByCubeName[cubeName];

    let preAggregationNode: t.ObjectExpression | null = null;
    const preAggregationAst = parse(`(${code})`);

    if (t.isExpressionStatement(preAggregationAst.program.body[0])) {
      const [statement] = preAggregationAst.program.body;

      if (t.isObjectExpression(statement.expression)) {
        preAggregationNode = statement.expression;
      }
    }

    if (preAggregationNode === null) {
      throw new Error('Pre-aggregation definition is malformed');
    }

    let anchor: t.ObjectExpression | null = null;

    cubeDefinition.properties.forEach((prop) => {
      if (t.isObjectProperty(prop) && t.isIdentifier(prop.key)) {
        if (prop.key.name === 'preAggregations' && t.isObjectExpression(prop.value)) {
          anchor = prop.value;

          prop.value.properties.forEach((p) => {
            if (t.isObjectProperty(p) && t.isIdentifier(p.key)) {
              if (p.key.name === preAggregationName) {
                throw new UserError(`Pre-aggregation '${preAggregationName}' is already defined`);
              }
            }
          });
        }
      }
    });

    if (anchor === null) {
      cubeDefinition.properties.push(
        t.objectProperty(
          t.identifier('preAggregations'),
          t.objectExpression([t.objectProperty(t.identifier(preAggregationName), preAggregationNode)])
        )
      );
    } else {
      (<t.ObjectExpression>anchor).properties.push(
        t.objectProperty(t.identifier(preAggregationName), <t.ObjectExpression>preAggregationNode)
      );
    }
  }
}
