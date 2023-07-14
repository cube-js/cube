import { MemberReference } from '../descriptors/MemberReference';
import { ValueWithComments } from '../descriptors/ValueWithComments';
import {
  SchemaDescriptor
} from '../ScaffoldingTemplate';
import { BaseSchemaFormatter } from './BaseSchemaFormatter';

function isPlainObject(value) {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  return Object.getPrototypeOf(value) === Object.getPrototypeOf({});
}

export class YamlSchemaFormatter extends BaseSchemaFormatter {
  public fileExtension(): string {
    return 'yml';
  }

  protected cubeReference(cube: string): string {
    return `{${cube}}`;
  }

  protected renderFile(fileDescriptor: Record<string, unknown>): string {
    const { cube, sql, preAggregations: _, ...descriptor } = fileDescriptor;

    return `cubes:\n  - name: ${cube}${this.render(
      {
        ...(sql ? { sql } : null),
        ...descriptor,
      },
      2
    )}\n`;
  }

  protected render(
    value: SchemaDescriptor,
    level = 0,
    parent?: SchemaDescriptor
  ) {
    const indent = Array(level * 2)
      .fill(0)
      .reduce((memo) => `${memo} `, '');

    if (value instanceof MemberReference) {
      return value.member;
    } else if (value instanceof ValueWithComments) {
      const comments = `\n${value.comments
        .map((comment) => `${indent}# ${comment}`)
        .join('\n')}\n`;

      return value.value ? `${this.render(value.value)}${comments}` : comments;
    } else if (Array.isArray(value)) {
      if (
        value.every(
          (v) => typeof v !== 'object' || v instanceof MemberReference
        )
      ) {
        return ` [${value.map(this.render).join(', ')}]\n`;
      }

      return `\n${value
        .map((v) => `${indent}- ${this.render(v, level + 1, value)}`)
        .join('\n')}`;
    } else if (typeof value === 'object') {
      if (parent) {
        return `${!Array.isArray(parent) ? '\n' : ''}${Object.entries(value)
          .map(
            ([k, v], index) => `${
              Array.isArray(parent) && index === 0 ? '' : `${indent}`
            }${k}:${this.render(v, level + 1, value)}`
          )
          .join('\n')}\n`;
      }

      const newLineKeys = Object.keys(value).includes('data_source') ? ['data_source'] : ['sql_table'];
      const content = Object.keys(value)
        .map((key) => {
          if (!isPlainObject(value[key])) {
            const newLine = newLineKeys.includes(key) ? '\n' : '';
            return `${indent}${key}:${this.render(
              value[key],
              level + 1,
              value
            )}${newLine}`;
          }

          return `${indent}${key}:${this.render(
            Object.entries(value[key] || {}).map(([ok, ov]) => ({
              name: ok,
              // @ts-ignore
              ...Object.entries(ov)
                .filter(([, v]) => v != null)
                .reduce((memo, [k, v]) => ({ ...memo, [k]: v }), {}),
            })),
            level + 1,
            value
          )}`;
        })
        .join('\n');

      return `\n${content}`;
    }

    return `${Array.isArray(parent) ? '' : ' '}${this.escapedValue(value)}`;
  }

  private escapedValue(value: string | number | boolean): string | number | boolean {
    if (typeof value !== 'string') {
      return value;
    }

    return value.match(/[{}"]/) ? `"${value.replace(/"/g, '\\"')}"` : value;
  }
}
