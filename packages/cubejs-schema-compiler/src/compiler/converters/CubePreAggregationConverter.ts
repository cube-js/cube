import { parse } from '@babel/parser';
import * as t from '@babel/types';

import { AstByCubeName, CubeConverterInterface } from './CubeSchemaConverter';

export type PreAggregationDefinition = {
  cubeName: string;
  preAggregationName: string;
  code: string;
};

export class CubePreAggregationConverter implements CubeConverterInterface {
  constructor(protected preAggregationDefinition: PreAggregationDefinition) {}

  convert(astByCubeName: AstByCubeName): void {
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

    // todo: insert `preAggregations` if it doesn't exist
    cubeDefinition.properties.forEach((prop) => {
      if (t.isObjectProperty(prop) && t.isIdentifier(prop.key)) {
        if (prop.key.name === 'preAggregations') {
          if (t.isObjectExpression(prop.value)) {
            prop.value.properties.push(
              t.objectProperty(t.identifier(preAggregationName), <t.ObjectExpression>preAggregationNode)
            );
          }
        }
      }
    });
  }
}
