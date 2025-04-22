import { parse } from '@babel/parser';
import * as t from '@babel/types';
import YAML, { isMap, isScalar, Scalar, YAMLMap, YAMLSeq, Pair, parseDocument } from 'yaml';

import { UserError } from '../UserError';

import { AstByCubeName, JsSet, CubeConverterInterface, YamlSet } from './CubeSchemaConverter';

export type PreAggregationDefinition = {
  cubeName: string;
  preAggregationName: string;
  code: string;
};

export class CubePreAggregationConverter implements CubeConverterInterface {
  public constructor(protected preAggregationDefinition: PreAggregationDefinition) {}

  public convert(astByCubeName: AstByCubeName): void {
    const { cubeName } = this.preAggregationDefinition;

    const cubeDefSet = astByCubeName[cubeName];

    if ('ast' in cubeDefSet) {
      this.convertJS(cubeDefSet);
    } else {
      this.convertYaml(cubeDefSet);
    }
  }

  protected convertJS(cubeDefSet: JsSet) {
    const { preAggregationName, code } = this.preAggregationDefinition;
    const { cubeDefinition } = cubeDefSet;

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

  protected convertYaml(cubeDefSet: YamlSet) {
    const { preAggregationName, code } = this.preAggregationDefinition;
    const { cubeDefinition } = cubeDefSet;

    const preAggDoc = YAML.parseDocument(code);
    const preAggNode = preAggDoc.contents;

    if (!preAggNode || !isMap(preAggNode)) {
      throw new UserError('Pre-aggregation YAML must be a map/object');
    }

    const preAggsPair = cubeDefinition.items.find(
      (pair: Pair) => isScalar(pair.key) && (pair.key.value === 'pre_aggregations' || pair.key.value === 'preAggregations')
    );

    if (preAggsPair) {
      const seq = preAggsPair.value;
      if (!YAML.isSeq(seq)) {
        throw new UserError('\'pre_aggregations\' must be a sequence');
      }

      const exists = seq.items.some(item => {
        if (isMap(item)) {
          const namePair = item.items.find(
            (pair: Pair) => isScalar(pair.key) && pair.key.value === 'name'
          );
          return namePair && isScalar(namePair.value) && namePair.value.value === preAggregationName;
        }
        return false;
      });

      if (exists) {
        throw new UserError(`Pre-aggregation '${preAggregationName}' is already defined`);
      }

      seq.items.push(preAggNode);
    } else {
      const newSeq = new YAMLSeq();
      newSeq.items.push(preAggNode);

      cubeDefinition.items.push(
        new Pair(new Scalar('pre_aggregations'), newSeq)
      );
    }
  }
}
