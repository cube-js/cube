import { MemberReference } from '../descriptors/MemberReference';
import { ValueWithComments } from '../descriptors/ValueWithComments';
import { SchemaDescriptor } from '../ScaffoldingTemplate';
import { BaseSchemaFormatter } from './BaseSchemaFormatter';

export class JavaScriptSchemaFormatter extends BaseSchemaFormatter {
  public fileExtension() {
    return 'js';
  }

  protected cubeReference(cube: string): string {
    return `\${${cube}}`;
  }

  protected renderFile(fileDescriptor: Record<string, unknown>): string {
    const { cube, ...descriptor } = fileDescriptor;
    return `cube(\`${cube}\`, ${this.render(descriptor, 0)});\n`;
  }

  protected render(descriptor: SchemaDescriptor, level: number, appendComment = ''): string {
    const lineSeparator = `,\n${level < 2 ? '\n' : ''}`;

    if (Array.isArray(descriptor)) {
      const items = descriptor.map(desc => this.render(desc, level + 1)).join(', ');
      return `[${items}]`;
    } else if (typeof descriptor === 'string') {
      return `\`${descriptor.replace(/`/g, '\\`')}\``;
    } else if (descriptor instanceof MemberReference) {
      return descriptor.member;
    } else if (descriptor instanceof ValueWithComments) {
      return this.render(
        descriptor.value,
        level,
        descriptor.comments.map((comment) => `  // ${comment}`).join('\n')
      );
    } else if (typeof descriptor === 'object') {
      const content = descriptor != null ? Object.keys(descriptor)
        .filter(k => descriptor[k] != null)
        .map(key => `${key}: ${this.render(descriptor[key], level + 1)}`)
        .join(lineSeparator)
        .split('\n')
        .map(l => `  ${l}`)
        .join('\n') : '';

      return `{\n${appendComment}${content}\n}`;
    }

    return descriptor.toString();
  }
}
